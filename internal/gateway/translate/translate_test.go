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

func TestUnsupportedFieldsFailLoudly(t *testing.T) {
	_, err := ParseAnthropicMessages([]byte(`{
		"model":"qwen2.5-9b-instruct",
		"max_tokens":4,
		"tool_choice":{"type":"auto"},
		"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]
	}`))
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
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
