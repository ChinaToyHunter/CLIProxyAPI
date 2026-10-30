package executor

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

func (e *XAIExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	ctx = helps.EnsureSessionContext(ctx, opts, req.Payload)
	if opts.Alt == "responses/compact" {
		return nil, statusErr{code: http.StatusBadRequest, msg: "streaming not supported for /responses/compact"}
	}
	if xaiIsSpeechRequest(opts) {
		return nil, statusErr{code: http.StatusBadRequest, msg: "streaming not supported for /audio/speech"}
	}
	if xaiInputHasItemType(req.Payload, "compaction_trigger") {
		return e.executeCompactionTriggerStream(ctx, auth, req, opts)
	}

	token, _ := xaiCreds(auth)
	baseURL := xaiChatBaseURL(auth)
	logXAIResolvedBaseURL(ctx, baseURL)

	prepared, err := e.prepareResponsesRequest(ctx, req, opts, true)
	if err != nil {
		return nil, err
	}

	reporter := helps.NewExecutorUsageReporter(ctx, e, prepared.baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)
	reporter.SetTranslatedReasoningEffort(prepared.body, e.Identifier())

	url := strings.TrimSuffix(baseURL, "/") + "/responses"
	prepared.body = sanitizeXAIInputEncryptedContentForBaseURL(prepared.body, baseURL)
	prepared.body = prepared.finalizePayload(prepared.body)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(prepared.body))
	if err != nil {
		return nil, err
	}
	applyXAIChatHeaders(httpReq, auth, token, true, prepared.sessionID, opts.Headers)
	e.recordXAIRequest(ctx, auth, url, httpReq.Header.Clone(), prepared.body)

	httpClient := helps.NewProxyAwareHTTPClient(ctx, e.cfg, auth, 0)
	httpClient = reporter.TrackHTTPClient(httpClient)
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return nil, err
	}
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		data, errRead := io.ReadAll(httpResp.Body)
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("xai executor: close response body error: %v", errClose)
		}
		if errRead != nil {
			helps.RecordAPIResponseError(ctx, e.cfg, errRead)
			return nil, errRead
		}
		helps.AppendAPIResponseChunk(ctx, e.cfg, data)
		helps.LogWithRequestID(ctx).Debugf("request error, error status: %d, error message: %s", httpResp.StatusCode, helps.SummarizeErrorBody(httpResp.Header.Get("Content-Type"), data))
		return nil, xaiStatusErr(httpResp.StatusCode, data)
	}

	out := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		defer close(out)
		defer func() {
			if errClose := httpResp.Body.Close(); errClose != nil {
				log.Errorf("xai executor: close response body error: %v", errClose)
			}
		}()
		scanner := helps.NewCodexSSEScanner(ctx, httpResp.Body, 52_428_800)
		scanner.SetOnLine(func(line []byte) { helps.AppendAPIResponseChunk(ctx, e.cfg, line) })
		claudeInputTokens := helps.NewClaudeInputTokenState(prepared.from, prepared.to, prepared.responseFormat, prepared.originalPayload)
		var streamUsage helps.StreamUsageBuffer
		defer streamUsage.Publish(ctx, reporter)
		fail := func(errFailure error) {
			streamUsage.PublishFailure(ctx, reporter, errFailure)
			select {
			case out <- cliproxyexecutor.StreamChunk{Err: errFailure}:
			case <-ctx.Done():
			}
		}
		var param any
		outputItemsByIndex := make(map[int64][]byte)
		var outputItemsFallback [][]byte
		responseFilter := newXAIInternalXSearchResponseFilter(prepared.filterInternalXSearch, prepared.clientDeclaredTools)
		namespaceRestorer := newXAINamespaceRestorer(prepared.namespaceTools)
		var toolStream *helps.XAIToolStream
		if prepared.responseFormat == sdktranslator.FormatOpenAI || prepared.responseFormat == sdktranslator.FormatClaude {
			delayedNames := make(map[string]bool)
			for name := range prepared.namespaceTools {
				delayedNames[name] = true
			}
			toolStream = helps.NewXAIToolStream(delayedNames)
		}
		var pendingEventLine []byte
		emitTranslatedLine := func(translatedLine []byte) bool {
			lines, errBridge := prepared.applyPatch.Stream(translatedLine)
			if errBridge != nil {
				streamUsage.PublishFailure(ctx, reporter, statusErr{code: http.StatusBadGateway, msg: helps.ApplyPatchUpstreamErrorMessage})
			}
			var chunks [][]byte
			for _, line := range lines {
				if bytes.HasPrefix(line, xaiDataTag) {
					eventData := bytes.TrimSpace(line[len(xaiDataTag):])
					switch gjson.GetBytes(eventData, "type").String() {
					case "response.output_item.done":
						xaiCollectOutputItemDone(eventData, outputItemsByIndex, &outputItemsFallback)
					case "response.completed", "response.incomplete":
						// Reconstruct only after the bridge has restored dispatcher children.
						eventData = xaiPatchCompletedOutput(eventData, outputItemsByIndex, outputItemsFallback)
						eventData = xaiNormalizeReasoningSummaryData(eventData)
					}
					if toolStream == nil {
						if gjson.GetBytes(eventData, "type").String() == "response.completed" {
							cacheXAIReasoningReplayFromCompleted(ctx, prepared.replayScope, eventData)
						}
						ending := line[len(bytes.TrimRight(line, "\r\n")):]
						line = append(append([]byte("data: "), eventData...), ending...)
						chunks = append(chunks, helps.TranslateStreamWithClaudeInputTokens(ctx, prepared.to, prepared.responseFormat, req.Model, prepared.originalPayload, prepared.body, line, &param, claudeInputTokens)...)
						continue
					}
					eventsForTranslation, errTool := toolStream.Transform(eventData)
					if errTool != nil {
						fail(statusErr{code: http.StatusBadGateway, msg: errTool.Error()})
						return false
					}
					if gjson.GetBytes(eventData, "type").String() == "response.completed" {
						// Do not cache a turn until the Grok-only identity adapter accepted it.
						cacheXAIReasoningReplayFromCompleted(ctx, prepared.replayScope, eventData)
					}
					for _, eventData := range eventsForTranslation {
						translated := append([]byte("data: "), eventData...)
						chunks = append(chunks, helps.TranslateStreamWithClaudeInputTokens(ctx, prepared.to, prepared.responseFormat, req.Model, prepared.originalPayload, prepared.body, translated, &param, claudeInputTokens)...)
					}
					continue
				}
				chunks = append(chunks, helps.TranslateStreamWithClaudeInputTokens(ctx, prepared.to, prepared.responseFormat, req.Model, prepared.originalPayload, prepared.body, line, &param, claudeInputTokens)...)
			}
			helps.RecordApplyPatchStreamFailureWithUsage(ctx, param, reporter, &streamUsage, statusErr{code: http.StatusBadGateway, msg: helps.ApplyPatchUpstreamErrorMessage})
			for i := range chunks {
				select {
				case out <- cliproxyexecutor.StreamChunk{Payload: chunks[i]}:
				case <-ctx.Done():
					return false
				}
			}
			if helps.StopApplyPatchStreamWithUsage(ctx, param, reporter, &streamUsage, out, statusErr{code: http.StatusBadGateway, msg: helps.ApplyPatchUpstreamErrorMessage}) {
				return false
			}
			if errBridge != nil {
				errBridge = statusErr{code: http.StatusBadGateway, msg: helps.ApplyPatchUpstreamErrorMessage}
				streamUsage.PublishFailure(ctx, reporter, errBridge)
				select {
				case out <- cliproxyexecutor.StreamChunk{Err: errBridge}:
				case <-ctx.Done():
				}
				return false
			}
			return true
		}
		sawTerminal := false
	streamLoop:
		for scanner.Scan() {
			line := scanner.Bytes()

			if bytes.HasPrefix(line, xaiEventTag) {
				if pendingEventLine != nil && !emitTranslatedLine(xaiNormalizeReasoningSummaryEventLine(pendingEventLine, "")) {
					return
				}
				pendingEventLine = bytes.Clone(line)
				continue
			}

			if bytes.HasPrefix(line, xaiDataTag) {
				eventDataList := xaiNormalizeReasoningSummaryDataEvents(bytes.TrimSpace(line[len(xaiDataTag):]))
				hasPendingEventLine := pendingEventLine != nil
				for i, eventData := range eventDataList {
					eventData = helps.NormalizeXAIResponseEvent(eventData)
					if bytes.Equal(eventData, []byte("[DONE]")) {
						if prepared.applyPatch.Active() && !emitTranslatedLine([]byte("data: [DONE]\n\n")) {
							return
						}
						fail(statusErr{code: http.StatusBadGateway, msg: "xai stream ended without a response terminal event"})
						return
					}
					if body, failed := codexTerminalFailureBody(eventData); failed {
						fail(xaiStatusErr(codexTerminalFailureStatus(body), body))
						return
					}
					prepared.applyPatch.RememberDispatcherEvent(eventData)
					eventData = namespaceRestorer.restore(eventData)
					if prepared.webSearchAlias != "" {
						eventData = restoreXAIClientWebSearchName(eventData, prepared.webSearchAlias)
					}
					eventData = responseFilter.apply(eventData)
					if len(eventData) == 0 {
						if hasPendingEventLine && i == 0 {
							pendingEventLine = nil
						}
						continue
					}
					reporter.ObserveResponseModel(eventData)
					normalizedEventName := gjson.GetBytes(eventData, "type").String()
					if normalizedEventName == "response.completed" || normalizedEventName == "response.incomplete" {
						if detail, ok := helps.ParseCodexUsage(eventData); ok {
							streamUsage.Observe(detail, true)
						}
					}

					if hasPendingEventLine {
						eventLine := []byte("event: " + normalizedEventName)
						if i == 0 {
							eventLine = xaiNormalizeReasoningSummaryEventLine(pendingEventLine, normalizedEventName)
							pendingEventLine = nil
						}
						if !emitTranslatedLine(eventLine) {
							return
						}
					}
					if !emitTranslatedLine(append([]byte("data: "), eventData...)) {
						return
					}
					if normalizedEventName == "response.completed" || normalizedEventName == "response.incomplete" {
						sawTerminal = true
						break streamLoop
					}
				}
				continue
			}

			if pendingEventLine != nil {
				if !emitTranslatedLine(xaiNormalizeReasoningSummaryEventLine(pendingEventLine, "")) {
					return
				}
				pendingEventLine = nil
			}
			if !emitTranslatedLine(bytes.Clone(line)) {
				return
			}
		}
		if pendingEventLine != nil {
			emitTranslatedLine(xaiNormalizeReasoningSummaryEventLine(pendingEventLine, ""))
		}
		finishEvents, errFinish := prepared.applyPatch.FinishStream()
		if errFinish != nil {
			streamUsage.PublishFailure(ctx, reporter, statusErr{code: http.StatusBadGateway, msg: helps.ApplyPatchUpstreamErrorMessage})
		}
		for _, event := range finishEvents {
			for _, chunk := range helps.TranslateStreamWithClaudeInputTokens(ctx, prepared.to, prepared.responseFormat, req.Model, prepared.originalPayload, prepared.body, event, &param, claudeInputTokens) {
				select {
				case out <- cliproxyexecutor.StreamChunk{Payload: chunk}:
				case <-ctx.Done():
					return
				}
			}
		}
		if errFinish != nil {
			errFinish = statusErr{code: http.StatusBadGateway, msg: helps.ApplyPatchUpstreamErrorMessage}
			streamUsage.PublishFailure(ctx, reporter, errFinish)
			select {
			case out <- cliproxyexecutor.StreamChunk{Err: errFinish}:
			case <-ctx.Done():
			}
			return
		}
		if sawTerminal && prepared.applyPatch.Active() && prepared.responseFormat == sdktranslator.FormatOpenAIResponse {
			// Preserve the validated patch bridge's transport marker without waiting
			// for another upstream read after semantic completion.
			if !emitTranslatedLine([]byte("data: [DONE]\n\n")) {
				return
			}
		}
		if errScan := scanner.Err(); errScan != nil {
			helps.RecordAPIResponseError(ctx, e.cfg, errScan)
			fail(errScan)
			return
		}
		if !sawTerminal {
			fail(statusErr{code: http.StatusRequestTimeout, msg: "xai stream error: stream disconnected before response.completed or response.incomplete"})
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: httpResp.Header.Clone(), Chunks: out}, nil
}
