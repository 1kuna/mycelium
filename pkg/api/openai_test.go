package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestOpenAIMessageUnmarshalStringAndContentParts(t *testing.T) {
	var text OpenAIMessage
	if err := json.Unmarshal([]byte(`{"role":"user","content":"hi"}`), &text); err != nil {
		t.Fatalf("unmarshal text: %v", err)
	}
	if text.Content != "hi" || len(text.ContentParts) != 0 {
		t.Fatalf("text = %+v", text)
	}

	var parts OpenAIMessage
	if err := json.Unmarshal([]byte(`{"role":"user","content":[{"type":"text","text":"hi"},{"type":"text","text":"there"},{"type":"image_url","image_url":{"url":"data:image/png;base64,xx"}}]}`), &parts); err != nil {
		t.Fatalf("unmarshal parts: %v", err)
	}
	if parts.Content != "hi\nthere" || len(parts.ContentParts) != 3 {
		t.Fatalf("parts = %+v", parts)
	}
}

func TestOpenAIMessageMarshalContentVariants(t *testing.T) {
	text, err := json.Marshal(OpenAIMessage{Role: "tool", ToolCallID: "call_1", Content: "done"})
	if err != nil {
		t.Fatalf("marshal text: %v", err)
	}
	if !strings.Contains(string(text), `"content":"done"`) || !strings.Contains(string(text), `"tool_call_id":"call_1"`) {
		t.Fatalf("text json = %s", text)
	}

	parts, err := json.Marshal(OpenAIMessage{Role: "user", ContentParts: []OpenAIContentPart{{Type: "text", Text: "hi"}}})
	if err != nil {
		t.Fatalf("marshal parts: %v", err)
	}
	if !strings.Contains(string(parts), `"content":[`) || !strings.Contains(string(parts), `"text":"hi"`) {
		t.Fatalf("parts json = %s", parts)
	}
}

func TestOpenAIMessageRejectsInvalidContentShape(t *testing.T) {
	var msg OpenAIMessage
	err := json.Unmarshal([]byte(`{"role":"user","content":{}}`), &msg)
	if err == nil || !strings.Contains(err.Error(), "content must be") {
		t.Fatalf("err = %v", err)
	}
}

func TestOpenAIMessageRejectsUnsupportedNestedFields(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "message", body: `{"role":"user","content":"hi","audio":{"id":"a"}}`},
		{name: "request refusal", body: `{"role":"assistant","content":"no","refusal":"policy"}`},
		{name: "content part", body: `{"role":"user","content":[{"type":"text","text":"hi","cache_control":{"type":"ephemeral"}}]}`},
		{name: "tool call function", body: `{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"search","arguments":"{}","strict":true}}]}`},
		{name: "function call", body: `{"role":"assistant","function_call":{"name":"search","arguments":"{}","strict":true}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var msg OpenAIMessage
			err := json.Unmarshal([]byte(tc.body), &msg)
			if err == nil || !strings.Contains(err.Error(), "unknown field") {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestOpenAIChatResponseAcceptsResponseRefusalField(t *testing.T) {
	var resp OpenAIChatResponse
	err := json.Unmarshal([]byte(`{
		"service_tier": null,
		"choices": [{
			"index": 0,
			"message": {
				"role": "assistant",
				"content": "hello",
				"refusal": null,
				"annotations": null,
				"audio": null,
				"reasoning": null,
				"reasoning_content": "hidden chain detail"
			},
			"logprobs": null,
			"finish_reason": "stop",
			"stop_reason": null,
			"token_ids": null
		}],
		"usage": {"completion_tokens": 1, "total_tokens": 4, "prompt_tokens_details": null},
		"prompt_logprobs": null,
		"prompt_token_ids": null,
		"kv_transfer_params": null
	}`), &resp)
	if err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if got := resp.Choices[0].Message.Content; got != "hello" {
		t.Fatalf("content = %q", got)
	}
}
