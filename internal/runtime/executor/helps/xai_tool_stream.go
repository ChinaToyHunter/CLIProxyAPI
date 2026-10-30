package helps

import (
	"errors"
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// XAIToolStream delays only unresolved tool identities before the shared
// translators see them. Native Responses events bypass this adapter entirely.
type XAIToolStream struct {
	delayedNames map[string]bool
	byKey        map[string]*xaiStreamTool
	tools        []*xaiStreamTool
}

type xaiStreamTool struct {
	item      []byte
	index     int64
	hasIndex  bool
	delayed   bool
	done      bool
	arguments strings.Builder
}

func NewXAIToolStream(delayedNames map[string]bool) *XAIToolStream {
	return &XAIToolStream{delayedNames: delayedNames, byKey: make(map[string]*xaiStreamTool)}
}

func xaiStreamToolKeys(event, item gjson.Result) []string {
	var keys []string
	for _, pair := range [][2]string{{"item_id", "id"}, {"call_id", "call_id"}} {
		value := item.Get(pair[1]).String()
		if value == "" {
			value = event.Get(pair[0]).String()
		}
		if value != "" {
			keys = append(keys, pair[0]+":"+value)
		}
	}
	if index := event.Get("output_index"); index.Exists() {
		keys = append(keys, fmt.Sprintf("index:%d", index.Int()))
	}
	return keys
}

func (s *XAIToolStream) tool(event, item gjson.Result, create bool) (*xaiStreamTool, error) {
	keys := xaiStreamToolKeys(event, item)
	var tool *xaiStreamTool
	for _, key := range keys {
		if matched := s.byKey[key]; matched != nil {
			if tool != nil && tool != matched {
				return nil, errors.New("xai stream error: conflicting tool call identities")
			}
			tool = matched
		}
	}
	if tool == nil && create {
		if len(keys) == 0 {
			return nil, errors.New("xai stream error: tool event has no identity")
		}
		tool = &xaiStreamTool{item: []byte(`{"type":"function_call"}`), delayed: true}
		s.tools = append(s.tools, tool)
	}
	if tool == nil {
		return nil, nil
	}
	for _, pair := range [][2]string{{"item_id", "id"}, {"call_id", "call_id"}} {
		value := item.Get(pair[1]).String()
		if value == "" {
			value = event.Get(pair[0]).String()
		}
		if old := gjson.GetBytes(tool.item, pair[1]).String(); value != "" && old != "" && value != old {
			return nil, errors.New("xai stream error: conflicting tool call identities")
		}
		if value != "" && !tool.done {
			tool.item, _ = sjson.SetBytes(tool.item, pair[1], value)
		}
	}
	for _, key := range keys {
		s.byKey[key] = tool
	}
	if index := event.Get("output_index"); index.Exists() {
		if tool.hasIndex && tool.index != index.Int() {
			return nil, errors.New("xai stream error: conflicting tool call indexes")
		}
		tool.index, tool.hasIndex = index.Int(), true
	}
	if item.Exists() && !tool.done {
		for _, field := range []string{"type", "name", "namespace", "arguments", "input", "status"} {
			if value := item.Get(field); value.Exists() && (value.Type != gjson.String || value.String() != "") {
				tool.item, _ = sjson.SetRawBytes(tool.item, field, []byte(value.Raw))
			}
		}
	}
	return tool, nil
}

func xaiIsStreamTool(item gjson.Result) bool {
	kind := item.Get("type").String()
	return kind == "function_call" || kind == "custom_tool_call"
}

func (s *XAIToolStream) finish(tool *xaiStreamTool, event []byte) ([][]byte, error) {
	if tool.done {
		return nil, nil
	}
	item := gjson.ParseBytes(tool.item)
	if item.Get("name").String() == "" || item.Get("call_id").String() == "" || (s.delayedNames[item.Get("name").String()] && item.Get("namespace").String() == "") {
		return nil, errors.New("xai stream error: tool call ended without a resolved name or call_id")
	}
	if item.Get("arguments").String() == "" && tool.arguments.Len() > 0 {
		tool.item, _ = sjson.SetBytes(tool.item, "arguments", tool.arguments.String())
	}
	out, _ := sjson.SetRawBytes(event, "item", tool.item)
	if tool.hasIndex {
		out, _ = sjson.SetBytes(out, "output_index", tool.index)
	}
	tool.done = true
	return [][]byte{out}, nil
}

func (s *XAIToolStream) Transform(data []byte) ([][]byte, error) {
	if s == nil {
		return [][]byte{data}, nil
	}
	root := gjson.ParseBytes(data)
	item := root.Get("item")
	switch root.Get("type").String() {
	case "response.output_item.added":
		if !xaiIsStreamTool(item) {
			break
		}
		existing, errTool := s.tool(root, item, false)
		if errTool != nil {
			return nil, errTool
		}
		if existing != nil {
			return nil, nil
		}
		tool, errTool := s.tool(root, item, true)
		if errTool != nil {
			return nil, errTool
		}
		tool.delayed = item.Get("name").String() == "" || item.Get("call_id").String() == "" || s.delayedNames[item.Get("name").String()]
		if tool.delayed {
			return nil, nil
		}
	case "response.function_call_arguments.delta", "response.custom_tool_call_input.delta":
		tool, errTool := s.tool(root, gjson.Result{}, true)
		if errTool != nil {
			return nil, errTool
		}
		if tool.done {
			return nil, nil
		}
		if tool.delayed {
			tool.arguments.WriteString(root.Get("delta").String())
			return nil, nil
		}
	case "response.function_call_arguments.done", "response.custom_tool_call_input.done":
		tool, errTool := s.tool(root, gjson.Result{}, true)
		if errTool != nil {
			return nil, errTool
		}
		field := "arguments"
		if root.Get("type").String() == "response.custom_tool_call_input.done" {
			field = "input"
		}
		if value := root.Get(field); value.Exists() && !tool.done {
			tool.item, _ = sjson.SetRawBytes(tool.item, field, []byte(value.Raw))
		}
		if tool.done || tool.delayed {
			return nil, nil
		}
	case "response.output_item.done":
		if xaiIsStreamTool(item) {
			tool, errTool := s.tool(root, item, true)
			if errTool != nil {
				return nil, errTool
			}
			return s.finish(tool, data)
		}
	case "response.completed", "response.incomplete":
		var out [][]byte
		for i, output := range root.Get("response.output").Array() {
			if !xaiIsStreamTool(output) {
				continue
			}
			done := []byte(`{"type":"response.output_item.done"}`)
			done, _ = sjson.SetRawBytes(done, "item", []byte(output.Raw))
			tool, errTool := s.tool(gjson.ParseBytes(done), output, false)
			if errTool != nil {
				return nil, errTool
			}
			if tool == nil {
				// Array position is only a fallback when no explicit identity matches.
				done, _ = sjson.SetBytes(done, "output_index", i)
				tool, errTool = s.tool(gjson.ParseBytes(done), output, true)
				if errTool != nil {
					return nil, errTool
				}
			}
			events, errFinish := s.finish(tool, done)
			if errFinish != nil {
				return nil, errFinish
			}
			out = append(out, events...)
		}
		for _, tool := range s.tools {
			if tool.done {
				continue
			}
			events, errFinish := s.finish(tool, []byte(`{"type":"response.output_item.done"}`))
			if errFinish != nil {
				return nil, errFinish
			}
			out = append(out, events...)
		}
		return append(out, data), nil
	}
	return [][]byte{data}, nil
}
