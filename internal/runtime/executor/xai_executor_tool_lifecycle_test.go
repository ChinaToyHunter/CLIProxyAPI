package executor

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

func xaiLoopToolEvent(kind string, item map[string]any) string {
	data, _ := json.Marshal(map[string]any{"type": kind, "output_index": 0, "item": item})
	return "data: " + string(data) + "\n\n"
}

func TestXAILoopNamespaceToolsUseRestoredIdentity(t *testing.T) {
	for _, format := range []sdktranslator.Format{sdktranslator.FormatOpenAI, sdktranslator.FormatClaude} {
		t.Run(string(format), func(t *testing.T) {
			added := map[string]any{"id": "fc_ns", "type": "function_call", "call_id": "call_ns", "name": "acme__lookup", "arguments": ""}
			done := map[string]any{"id": "fc_ns", "type": "function_call", "call_id": "call_ns", "name": "acme__lookup", "arguments": `{"q":"fixture"}`}
			events := xaiLoopToolEvent("response.output_item.added", added) + xaiLoopToolEvent("response.output_item.done", done) + xaiLoopTerminal("completed")
			request := `{"model":"grok-4.5","input":"fixture","tools":[{"type":"namespace","name":"acme","tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}]}`
			payload, err := xaiLoopRunFixture(t, events, true, true, sdktranslator.FormatOpenAIResponse, format, request)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(payload), "acme__lookup") || !strings.Contains(string(payload), `"name":"lookup"`) {
				t.Fatalf("qualified upstream identity leaked instead of restored tool: %s", payload)
			}
		})
	}
}

func TestXAILoopTranslatedToolsUseFinalIdentityOnce(t *testing.T) {
	for _, format := range []sdktranslator.Format{sdktranslator.FormatOpenAI, sdktranslator.FormatClaude} {
		for _, terminalOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/terminal_only=%t", format, terminalOnly), func(t *testing.T) {
				added := map[string]any{"id": "fc_loop", "type": "function_call", "call_id": "call_loop", "arguments": ""}
				item := map[string]any{"id": "fc_loop", "type": "function_call", "call_id": "call_loop", "name": "lookup", "arguments": `{"q":"synthetic"}`}
				events := xaiLoopToolEvent("response.output_item.added", added)
				events += "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_loop\",\"output_index\":0,\"delta\":\"{\\\"q\\\":\\\"synthetic\\\"}\"}\n\n"
				if !terminalOnly {
					done := xaiLoopToolEvent("response.output_item.done", item)
					events += done + done
				}
				terminal, _ := json.Marshal(map[string]any{"type": "response.completed", "response": map[string]any{"id": "resp_loop", "status": "completed", "output": []any{item}}})
				events += "data: " + string(terminal) + "\n\n"
				payload, err := xaiLoopRunFixture(t, events, true, true, sdktranslator.FormatOpenAIResponse, format, `{"model":"grok-4.5","input":"fixture","tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}`)
				if err != nil {
					t.Fatal(err)
				}
				var names []string
				var arguments strings.Builder
				for _, line := range strings.Split(string(payload), "\n") {
					line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
					if format == sdktranslator.FormatOpenAI {
						for _, call := range gjson.Get(line, "choices.0.delta.tool_calls").Array() {
							if call.Get("id").String() != "" {
								names = append(names, call.Get("function.name").String())
							}
							arguments.WriteString(call.Get("function.arguments").String())
						}
					} else {
						if gjson.Get(line, "content_block.type").String() == "tool_use" {
							names = append(names, gjson.Get(line, "content_block.name").String())
						}
						arguments.WriteString(gjson.Get(line, "delta.partial_json").String())
					}
				}
				if len(names) != 1 || names[0] != "lookup" || arguments.String() != `{"q":"synthetic"}` {
					t.Fatalf("names=%q arguments=%q; want one lookup with intact arguments", names, arguments.String())
				}
			})
		}
	}
}
