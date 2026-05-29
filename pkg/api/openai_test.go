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
