package helps

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestXAIToolStreamKeepsDistinctCallsAndDropsOnlyDuplicateLifecycleEvents(t *testing.T) {
	s := NewXAIToolStream(nil)
	var calls []string
	for _, event := range []string{
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"first","call_id":"call_first","name":"lookup"}}`,
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"first","call_id":"call_first","name":"lookup"}}`,
		`{"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","id":"second","call_id":"call_second","name":"lookup"}}`,
		`{"type":"response.output_item.done","output_index":1,"item":{"type":"function_call","id":"second","call_id":"call_second","name":"lookup","arguments":"{}"}}`,
		`{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"first","call_id":"call_first","name":"lookup","arguments":"{}"}}`,
		`{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"first","call_id":"call_first","name":"lookup","arguments":"{}"}}`,
	} {
		out, err := s.Transform([]byte(event))
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range out {
			if gjson.GetBytes(event, "type").String() == "response.output_item.done" {
				calls = append(calls, gjson.GetBytes(event, "item.call_id").String())
			}
		}
	}
	if got := strings.Join(calls, ","); got != "call_second,call_first" {
		t.Fatalf("distinct same-name calls changed: %s", got)
	}
}

func TestXAIToolStreamUnresolvedIdentityFailsWithoutEchoingArguments(t *testing.T) {
	s := NewXAIToolStream(nil)
	_, _ = s.Transform([]byte(`{"type":"response.output_item.added","output_index":0,"item":{"id":"one","type":"function_call"}}`))
	_, err := s.Transform([]byte(`{"type":"response.completed","response":{"output":[{"id":"one","type":"function_call","arguments":"PRIVATE_TEXT"}]}}`))
	if err == nil || strings.Contains(err.Error(), "PRIVATE_TEXT") {
		t.Fatalf("expected non-sensitive unresolved identity error, got %v", err)
	}
}

func TestXAIToolStreamSparseTerminalKeepsParallelIndexes(t *testing.T) {
	s := NewXAIToolStream(nil)
	for _, event := range []string{
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"first","call_id":"a"}}`,
		`{"type":"response.output_item.added","output_index":4,"item":{"type":"function_call","id":"second","call_id":"b"}}`,
	} {
		if _, err := s.Transform([]byte(event)); err != nil {
			t.Fatal(err)
		}
	}
	out, err := s.Transform([]byte(`{"type":"response.completed","response":{"output":[{"type":"function_call","id":"second","call_id":"b","name":"second_tool","arguments":"{}"},{"type":"function_call","id":"first","call_id":"a","name":"first_tool","arguments":"{}"}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range out {
		if gjson.GetBytes(event, "type").String() != "response.output_item.done" {
			continue
		}
		want := int64(0)
		if gjson.GetBytes(event, "item.id").String() == "second" {
			want = 4
		}
		if got := gjson.GetBytes(event, "output_index").Int(); got != want {
			t.Fatalf("sparse terminal reassigned tool index: %s, want %d", event, want)
		}
	}
}

func TestXAIToolStreamArgumentsBeforeAddedDoNotLeak(t *testing.T) {
	s := NewXAIToolStream(nil)
	for _, event := range []string{
		`{"type":"response.function_call_arguments.delta","item_id":"late","delta":"{\"q\":1}"}`,
		`{"type":"response.output_item.added","output_index":2,"item":{"id":"late","type":"function_call","call_id":"call_late","name":"lookup"}}`,
	} {
		out, err := s.Transform([]byte(event))
		if err != nil || len(out) != 0 {
			t.Fatalf("unresolved identity escaped: %s %v", out, err)
		}
	}
	out, err := s.Transform([]byte(`{"type":"response.output_item.done","item":{"id":"late","type":"function_call","call_id":"call_late","name":"lookup"}}`))
	if err != nil || len(out) != 1 || gjson.GetBytes(out[0], "item.arguments").String() != `{"q":1}` {
		t.Fatalf("early arguments not hydrated: %s %v", out, err)
	}
}

func TestXAIToolStreamRejectsConflictingParallelIdentity(t *testing.T) {
	s := NewXAIToolStream(nil)
	_, _ = s.Transform([]byte(`{"type":"response.output_item.added","output_index":0,"item":{"id":"first","type":"function_call","call_id":"a","name":"lookup"}}`))
	_, _ = s.Transform([]byte(`{"type":"response.output_item.added","output_index":1,"item":{"id":"second","type":"function_call","call_id":"b","name":"lookup"}}`))
	_, err := s.Transform([]byte(`{"type":"response.function_call_arguments.delta","item_id":"first","call_id":"b","delta":"PRIVATE_TEXT"}`))
	if err == nil || strings.Contains(err.Error(), "PRIVATE_TEXT") {
		t.Fatalf("expected clean conflicting identity error, got %v", err)
	}
}

func TestNormalizeXAIResponseDonePreservesFailureStatus(t *testing.T) {
	for _, kind := range []string{"completed", "incomplete", "failed"} {
		event := []byte(`{"type":"response.done","response":{"status":"` + kind + `"}}`)
		got := NormalizeXAIResponseEvent(event)
		if gjson.GetBytes(got, "type").String() != "response."+kind {
			t.Fatalf("wrong terminal kind: %s", got)
		}
	}
}
