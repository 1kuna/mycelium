package translate

import (
	"encoding/json"
	"strings"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/gateway/profiles"
	"mycelium/pkg/api"
)

func TestAnthropicMessagesTranslateToOpenAIChat(t *testing.T) {
	req, err := ParseAnthropicMessages([]byte(`{
		"model":"qwen2.5-9b-instruct",
		"system":"be terse",
		"max_tokens":4,
		"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]
	}`))
	if err != nil {
		t.Fatalf("ParseAnthropicMessages: %v", err)
	}
	profile, err := profiles.DefaultRegistry().ForBackend(domain.BackendLlamaCpp)
	if err != nil {
		t.Fatalf("ForBackend: %v", err)
	}
	upstream, err := BuildUpstream(req, profile)
	if err != nil {
		t.Fatalf("BuildUpstream: %v", err)
	}
	if !upstream.Translate || upstream.Path != "/v1/chat/completions" {
		t.Fatalf("upstream = %+v", upstream)
	}
	var openai api.OpenAIChatRequest
	if err := json.Unmarshal(upstream.Body, &openai); err != nil {
		t.Fatalf("unmarshal upstream: %v", err)
	}
	if len(openai.Messages) != 2 || openai.Messages[0].Role != "system" || openai.Messages[1].Content != "hi" {
		t.Fatalf("messages = %+v", openai.Messages)
	}
}

func TestOpenAIParseAndBuildUpstream(t *testing.T) {
	profile := profiles.Profile{
		ID:             "openai",
		Backend:        domain.BackendLlamaCpp,
		Format:         profiles.FormatOpenAI,
		ChatPath:       "/chat",
		CompletionPath: "/complete",
	}
	chat, err := ParseOpenAIChat([]byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	route, err := BuildUpstream(chat, profile)
	if err != nil {
		t.Fatalf("BuildUpstream chat: %v", err)
	}
	if route.Path != "/chat" || string(route.Body) != string(chat.Body) {
		t.Fatalf("chat route = %+v", route)
	}
	completion, err := ParseOpenAICompletion([]byte(`{"model":"m","prompt":"hi","max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAICompletion: %v", err)
	}
	route, err = BuildUpstream(completion, profile)
	if err != nil {
		t.Fatalf("BuildUpstream completion: %v", err)
	}
	if route.Path != "/complete" || string(route.Body) != string(completion.Body) {
		t.Fatalf("completion route = %+v", route)
	}
	body, contentType, err := TranslateResponse(chat, route, []byte(`{"ok":true}`))
	if err != nil || contentType != "application/json" || string(body) != `{"ok":true}` {
		t.Fatalf("TranslateResponse passthrough = %q %s %v", body, contentType, err)
	}
}

func TestOpenAIChatAcceptsContentPartsAndTools(t *testing.T) {
	raw := `{
		"model":"m",
		"messages":[
			{"role":"user","content":[{"type":"text","text":"hi"},{"type":"image_url","image_url":{"url":"data:image/png;base64,xx"}}]},
			{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]},
			{"role":"tool","tool_call_id":"call_1","content":"done"}
		],
		"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}],
		"tool_choice":"auto"
	}`
	req, err := ParseOpenAIChat([]byte(raw))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	if req.OpenAI.Messages[0].Content != "hi" || len(req.OpenAI.Messages[0].ContentParts) != 2 || len(req.OpenAI.Tools) != 1 || len(req.OpenAI.Messages[1].ToolCalls) != 1 {
		t.Fatalf("parsed request = %+v", req.OpenAI)
	}
	profile := profiles.Profile{Format: profiles.FormatOpenAI, ChatPath: "/chat"}
	route, err := BuildUpstream(req, profile)
	if err != nil {
		t.Fatalf("BuildUpstream: %v", err)
	}
	if string(route.Body) != string(req.Body) {
		t.Fatalf("route body changed: %s", route.Body)
	}
}

func TestParseRequestsFailLoudly(t *testing.T) {
	cases := []struct {
		name string
		fn   func([]byte) (IngressRequest, error)
		body string
		want string
	}{
		{name: "chat model", fn: ParseOpenAIChat, body: `{"messages":[{"role":"user","content":"hi"}]}`, want: "model is required"},
		{name: "chat messages", fn: ParseOpenAIChat, body: `{"model":"m"}`, want: "messages are required"},
		{name: "completion model", fn: ParseOpenAICompletion, body: `{"prompt":"hi"}`, want: "model is required"},
		{name: "anthropic model", fn: ParseAnthropicMessages, body: `{"max_tokens":1,"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`, want: "model is required"},
		{name: "anthropic max tokens", fn: ParseAnthropicMessages, body: `{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`, want: "max_tokens is required"},
		{name: "anthropic messages", fn: ParseAnthropicMessages, body: `{"model":"m","max_tokens":1}`, want: "messages are required"},
		{name: "multiple json", fn: ParseOpenAICompletion, body: `{"model":"m"} {}`, want: "multiple JSON"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.fn([]byte(tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v want %q", err, tc.want)
			}
		})
	}
}

func TestBuildAndTranslateUnsupportedRoutesFailLoudly(t *testing.T) {
	chat, err := ParseOpenAIChat([]byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	unsupported := profiles.Profile{Format: profiles.FormatAnthropic}
	if _, err := BuildUpstream(chat, unsupported); err == nil || !strings.Contains(err.Error(), "openai chat") {
		t.Fatalf("chat err = %v", err)
	}
	completion, err := ParseOpenAICompletion([]byte(`{"model":"m","prompt":"hi"}`))
	if err != nil {
		t.Fatalf("ParseOpenAICompletion: %v", err)
	}
	if _, err := BuildUpstream(completion, unsupported); err == nil || !strings.Contains(err.Error(), "openai completion") {
		t.Fatalf("completion err = %v", err)
	}
	claude, err := ParseAnthropicMessages([]byte(`{"model":"m","max_tokens":1,"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`))
	if err != nil {
		t.Fatalf("ParseAnthropicMessages: %v", err)
	}
	if _, err := BuildUpstream(claude, profiles.Profile{Format: "custom"}); err == nil || !strings.Contains(err.Error(), "anthropic messages") {
		t.Fatalf("anthropic err = %v", err)
	}
	if _, _, err := TranslateResponse(chat, UpstreamRequest{Translate: true}, nil); err == nil || !strings.Contains(err.Error(), "unsupported translated response") {
		t.Fatalf("translate err = %v", err)
	}
	if _, _, err := TranslateResponse(claude, UpstreamRequest{Translate: true}, []byte(`{`)); err == nil {
		t.Fatal("expected bad upstream json")
	}
	if _, _, err := TranslateResponse(claude, UpstreamRequest{Translate: true}, []byte(`{"choices":[]}`)); err != nil {
		t.Fatalf("empty translated response should be valid: %v", err)
	}
}

func TestUnknownFieldsFailLoudly(t *testing.T) {
	_, err := ParseAnthropicMessages([]byte(`{
		"model":"qwen2.5-9b-instruct",
		"max_tokens":4,
		"banana":true,
		"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]
	}`))
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("err = %v", err)
	}
}

func TestAnthropicToolsPassThroughForAnthropicProfile(t *testing.T) {
	raw := `{
		"model":"claude-local",
		"max_tokens":4,
		"tools":[{"name":"lookup","description":"look up a value","input_schema":{"type":"object"}}],
		"tool_choice":{"type":"tool","name":"lookup"},
		"messages":[
			{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"lookup","input":{"q":"hi"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"done"}]}
		]
	}`
	req, err := ParseAnthropicMessages([]byte(raw))
	if err != nil {
		t.Fatalf("ParseAnthropicMessages: %v", err)
	}
	if len(req.Claude.Tools) != 1 || req.Claude.ToolChoice == nil || req.Claude.Messages[0].Content[0].Type != "tool_use" {
		t.Fatalf("parsed anthropic request = %+v", req.Claude)
	}
	route, err := BuildUpstream(req, profiles.Profile{Format: profiles.FormatAnthropic, AnthropicPath: "/messages"})
	if err != nil {
		t.Fatalf("BuildUpstream: %v", err)
	}
	if route.Path != "/messages" || string(route.Body) != string(req.Body) {
		t.Fatalf("route = %+v", route)
	}
}

func TestAnthropicToolsFailForOpenAITranslation(t *testing.T) {
	req, err := ParseAnthropicMessages([]byte(`{
		"model":"qwen2.5-9b-instruct",
		"max_tokens":4,
		"tools":[{"name":"lookup","input_schema":{"type":"object"}}],
		"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]
	}`))
	if err != nil {
		t.Fatalf("ParseAnthropicMessages: %v", err)
	}
	profile, err := profiles.DefaultRegistry().ForBackend(domain.BackendLlamaCpp)
	if err != nil {
		t.Fatalf("ForBackend: %v", err)
	}
	if _, err := BuildUpstream(req, profile); err == nil || !strings.Contains(err.Error(), "tool use cannot be translated") {
		t.Fatalf("err = %v", err)
	}
}

func TestAnthropicNonTextBlocksFailForOpenAITranslation(t *testing.T) {
	req, err := ParseAnthropicMessages([]byte(`{
		"model":"qwen2.5-9b-instruct",
		"max_tokens":4,
		"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"xx"}}]}]
	}`))
	if err != nil {
		t.Fatalf("ParseAnthropicMessages: %v", err)
	}
	profile, err := profiles.DefaultRegistry().ForBackend(domain.BackendLlamaCpp)
	if err != nil {
		t.Fatalf("ForBackend: %v", err)
	}
	if _, err := BuildUpstream(req, profile); err == nil || !strings.Contains(err.Error(), "cannot be translated") {
		t.Fatalf("err = %v", err)
	}
}

func TestStreamingTranslationFailsLoudly(t *testing.T) {
	req, err := ParseAnthropicMessages([]byte(`{
		"model":"qwen2.5-9b-instruct",
		"max_tokens":4,
		"stream":true,
		"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]
	}`))
	if err != nil {
		t.Fatalf("ParseAnthropicMessages: %v", err)
	}
	profile, err := profiles.DefaultRegistry().ForBackend(domain.BackendLlamaCpp)
	if err != nil {
		t.Fatalf("ForBackend: %v", err)
	}
	_, err = BuildUpstream(req, profile)
	if err == nil || !strings.Contains(err.Error(), "streaming anthropic-to-openai translation") {
		t.Fatalf("err = %v", err)
	}
}

func TestTranslatedOpenAIResponseBecomesAnthropicMessage(t *testing.T) {
	req, err := ParseAnthropicMessages([]byte(`{
		"model":"qwen2.5-9b-instruct",
		"max_tokens":4,
		"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]
	}`))
	if err != nil {
		t.Fatalf("ParseAnthropicMessages: %v", err)
	}
	route := UpstreamRequest{Translate: true}
	body, contentType, err := TranslateResponse(req, route, []byte(`{
		"id":"chatcmpl-test",
		"model":"qwen2.5-9b-instruct",
		"choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],
		"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}
	}`))
	if err != nil {
		t.Fatalf("TranslateResponse: %v", err)
	}
	if contentType != "application/json" {
		t.Fatalf("contentType = %s", contentType)
	}
	var out api.AnthropicMessagesResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal translated response: %v", err)
	}
	if out.Content[0].Text != "hello" || out.Usage.InputTokens != 3 || out.Usage.OutputTokens != 1 {
		t.Fatalf("out = %+v", out)
	}
}
