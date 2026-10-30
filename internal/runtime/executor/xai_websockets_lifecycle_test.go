package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestXAIWebsocketLoopIncompleteReleasesTurnWithoutDisconnect(t *testing.T) {
	release := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade: %v", errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("read: %v", errRead)
			return
		}
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.incomplete","response":{"id":"resp_loop_ws","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[]}}`)); errWrite != nil {
			t.Errorf("write: %v", errWrite)
			return
		}
		<-release
	}))
	defer server.Close()
	defer close(release)

	exec := NewXAIWebsocketsExecutor(&config.Config{})
	exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	exec.idStore = &xaiWebsocketIDStateStore{sessions: make(map[string]*xaiWebsocketIDState)}
	auth := &cliproxyauth.Auth{ID: "loop-ws", Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "websockets": "true"}, Metadata: map[string]any{"access_token": "synthetic"}}
	ctx, cancel := context.WithCancel(cliproxyexecutor.WithDownstreamWebsocket(context.Background()))
	defer cancel()
	result, err := exec.ExecuteStream(ctx, auth, cliproxyexecutor.Request{Model: "grok-4.5", Payload: []byte(`{"model":"grok-4.5","input":"fixture"}`)}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, ResponseFormat: sdktranslator.FormatOpenAIResponse})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case chunk := <-result.Chunks:
		if chunk.Err != nil || gjson.GetBytes(chunk.Payload, "type").String() != "response.incomplete" {
			t.Fatalf("wrong incomplete event: %s %v", chunk.Payload, chunk.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no incomplete event")
	}
	select {
	case _, open := <-result.Chunks:
		if open {
			t.Fatal("unexpected extra event")
		}
	case <-time.After(time.Second):
		t.Fatal("incomplete turn still owns the websocket reader")
	}
}
