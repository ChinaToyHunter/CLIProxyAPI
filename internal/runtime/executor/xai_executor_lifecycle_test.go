package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	_ "github.com/router-for-me/CLIProxyAPI/v8/internal/translator"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

// Detect an extra read deterministically instead of timing how long a socket hangs.
type xaiLoopTestBody struct {
	prefix    *strings.Reader
	hold      bool
	blocked   chan struct{}
	released  chan struct{}
	blockOnce sync.Once
	closeOnce sync.Once
}

func (b *xaiLoopTestBody) Read(p []byte) (int, error) {
	if b.prefix.Len() > 0 {
		return b.prefix.Read(p)
	}
	if b.hold {
		b.blockOnce.Do(func() { close(b.blocked) })
		<-b.released
	}
	return 0, io.EOF
}

func (b *xaiLoopTestBody) Close() error {
	b.closeOnce.Do(func() { close(b.released) })
	return nil
}

type xaiLoopTestTransport struct{ body *xaiLoopTestBody }

func (r xaiLoopTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Host != "grok-loop-test.invalid" {
		return nil, fmt.Errorf("unexpected test host %q", req.URL.Host)
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: r.body, Request: req}, nil
}

func xaiLoopRunFixture(t *testing.T, events string, hold, stream bool, source, response sdktranslator.Format, request string) ([]byte, error) {
	t.Helper()
	body := &xaiLoopTestBody{prefix: strings.NewReader(events), hold: hold, blocked: make(chan struct{}), released: make(chan struct{})}
	t.Cleanup(func() { _ = body.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, "cliproxy.roundtripper", xaiLoopTestTransport{body})
	exec := NewXAIExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: "synthetic-loop-test", Provider: "xai", Attributes: map[string]string{"base_url": "https://grok-loop-test.invalid"}, Metadata: map[string]any{"access_token": "synthetic"}}
	if request == "" {
		request = `{"model":"grok-4.5","input":"synthetic fixture"}`
	}
	req := cliproxyexecutor.Request{Model: "grok-4.5", Payload: []byte(request)}
	opts := cliproxyexecutor.Options{SourceFormat: source, ResponseFormat: response, Stream: stream}
	type outcome struct {
		payload []byte
		err     error
	}
	done := make(chan outcome, 1)
	if stream {
		result, err := exec.ExecuteStream(ctx, auth, req, opts)
		if err != nil {
			return nil, err
		}
		go func() {
			var out bytes.Buffer
			var firstErr error
			for chunk := range result.Chunks {
				if chunk.Err != nil && firstErr == nil {
					firstErr = chunk.Err
				}
				out.Write(chunk.Payload)
				out.WriteByte('\n')
			}
			done <- outcome{out.Bytes(), firstErr}
		}()
	} else {
		go func() {
			result, err := exec.Execute(ctx, auth, req, opts)
			done <- outcome{result.Payload, err}
		}()
	}
	select {
	case got := <-done:
		return got.payload, got.err
	case <-body.blocked:
		t.Error("executor read beyond the terminal event and waited for EOF")
		_ = body.Close()
		got := <-done
		return got.payload, got.err
	case <-time.After(15 * time.Second):
		_ = body.Close()
		t.Fatal("bounded fixture watchdog expired")
		return nil, nil
	}
}

func xaiLoopTerminal(kind string) string {
	return fmt.Sprintf("data: {\"type\":\"response.%s\",\"response\":{\"id\":\"resp_loop\",\"object\":\"response\",\"created_at\":0,\"status\":\"%s\",\"model\":\"grok-4.5\",\"output\":[],\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n", kind, kind)
}

func TestXAILoopSemanticTerminalClosesWithoutEOF(t *testing.T) {
	for _, kind := range []string{"completed", "incomplete"} {
		for _, stream := range []bool{false, true} {
			for _, hold := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/stream=%t/hold=%t", kind, stream, hold), func(t *testing.T) {
					_, err := xaiLoopRunFixture(t, xaiLoopTerminal(kind), hold, stream, sdktranslator.FormatOpenAIResponse, sdktranslator.FormatOpenAIResponse, "")
					if err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestXAILoopFailureTerminatesWithoutEOF(t *testing.T) {
	for _, event := range []string{
		`{"type":"response.failed","response":{"status":"failed","error":{"code":"invalid_request_error","message":"synthetic failure"}}}`,
		`{"type":"error","error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"synthetic failure"}}`,
		`[DONE]`,
	} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", gjson.Get(event, "type").String(), stream), func(t *testing.T) {
				payload, err := xaiLoopRunFixture(t, "data: "+event+"\n\n", true, stream, sdktranslator.FormatOpenAIResponse, sdktranslator.FormatOpenAI, "")
				if err == nil {
					t.Fatalf("failure reported success: %s", payload)
				}
			})
		}
	}
}

func TestXAILoopSplitTerminalAndPrematureEOF(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("split/stream=%t", stream), func(t *testing.T) {
			event := "event: response.completed\ndata: {\"type\":\"response.completed\",\ndata: \"response\":{\"id\":\"split\",\"status\":\"completed\",\"output\":[]}}\n\n"
			if _, err := xaiLoopRunFixture(t, event, true, stream, sdktranslator.FormatOpenAIResponse, sdktranslator.FormatOpenAIResponse, ""); err != nil {
				t.Fatal(err)
			}
		})
		t.Run(fmt.Sprintf("truncated/stream=%t", stream), func(t *testing.T) {
			if _, err := xaiLoopRunFixture(t, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n", false, stream, sdktranslator.FormatOpenAIResponse, sdktranslator.FormatOpenAI, ""); err == nil {
				t.Fatal("truncated upstream must not succeed")
			}
		})
	}
}

func TestXAILoopRepeatedTextBeforeTerminalIsPreserved(t *testing.T) {
	before := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"same phrase\"}\n\n"
	after := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"must not be emitted\"}\n\n"
	payload, err := xaiLoopRunFixture(t, before+before+xaiLoopTerminal("completed")+after, false, true, sdktranslator.FormatOpenAIResponse, sdktranslator.FormatOpenAI, "")
	if err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	for _, line := range strings.Split(string(payload), "\n") {
		text.WriteString(gjson.Get(strings.TrimSpace(line), "choices.0.delta.content").String())
	}
	if got := text.String(); got != "same phrasesame phrase" {
		t.Fatalf("text=%q: preserve legitimate repetition, ignore events after completion", got)
	}
}
