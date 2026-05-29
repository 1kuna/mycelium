package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type OpenAIChatRequest struct {
	Model       string          `json:"model"`
	Messages    []OpenAIMessage `json:"messages"`
	MaxTokens   int             `json:"max_tokens,omitempty"`
	Stream      bool            `json:"stream,omitempty"`
	Temperature *float64        `json:"temperature,omitempty"`
	Tools       []OpenAITool    `json:"tools,omitempty"`
	ToolChoice  json.RawMessage `json:"tool_choice,omitempty"`
}

type OpenAICompletionRequest struct {
	Model     string `json:"model"`
	Prompt    string `json:"prompt"`
	MaxTokens int    `json:"max_tokens,omitempty"`
	Stream    bool   `json:"stream,omitempty"`
}

type OpenAIMessage struct {
	Role         string              `json:"role"`
	Content      string              `json:"content,omitempty"`
	ContentParts []OpenAIContentPart `json:"-"`
	Name         string              `json:"name,omitempty"`
	ToolCallID   string              `json:"tool_call_id,omitempty"`
	ToolCalls    []OpenAIToolCall    `json:"tool_calls,omitempty"`
	FunctionCall *OpenAIFunctionCall `json:"function_call,omitempty"`
}

type OpenAIContentPart struct {
	Type     string          `json:"type"`
	Text     string          `json:"text,omitempty"`
	ImageURL json.RawMessage `json:"image_url,omitempty"`
}

type OpenAITool struct {
	Type     string         `json:"type"`
	Function OpenAIFunction `json:"function,omitempty"`
}

type OpenAIFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type OpenAIToolCall struct {
	ID       string             `json:"id"`
	Type     string             `json:"type"`
	Function OpenAIFunctionCall `json:"function,omitempty"`
}

type OpenAIFunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

func (m *OpenAIMessage) UnmarshalJSON(data []byte) error {
	var raw struct {
		Role         string              `json:"role"`
		Content      json.RawMessage     `json:"content"`
		Name         string              `json:"name"`
		ToolCallID   string              `json:"tool_call_id"`
		ToolCalls    []OpenAIToolCall    `json:"tool_calls"`
		FunctionCall *OpenAIFunctionCall `json:"function_call"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	m.Role = raw.Role
	m.Name = raw.Name
	m.ToolCallID = raw.ToolCallID
	m.ToolCalls = raw.ToolCalls
	m.FunctionCall = raw.FunctionCall
	if len(raw.Content) == 0 || bytes.Equal(raw.Content, []byte("null")) {
		return nil
	}
	if raw.Content[0] == '"' {
		if err := json.Unmarshal(raw.Content, &m.Content); err != nil {
			return err
		}
		return nil
	}
	if raw.Content[0] == '[' {
		if err := json.Unmarshal(raw.Content, &m.ContentParts); err != nil {
			return err
		}
		text := make([]string, 0, len(m.ContentParts))
		for _, part := range m.ContentParts {
			if part.Type == "text" {
				text = append(text, part.Text)
			}
		}
		m.Content = strings.Join(text, "\n")
		return nil
	}
	return fmt.Errorf("openai message content must be a string, array, or null")
}

func (m OpenAIMessage) MarshalJSON() ([]byte, error) {
	raw := struct {
		Role         string              `json:"role"`
		Content      any                 `json:"content,omitempty"`
		Name         string              `json:"name,omitempty"`
		ToolCallID   string              `json:"tool_call_id,omitempty"`
		ToolCalls    []OpenAIToolCall    `json:"tool_calls,omitempty"`
		FunctionCall *OpenAIFunctionCall `json:"function_call,omitempty"`
	}{
		Role:         m.Role,
		Name:         m.Name,
		ToolCallID:   m.ToolCallID,
		ToolCalls:    m.ToolCalls,
		FunctionCall: m.FunctionCall,
	}
	if len(m.ContentParts) > 0 {
		raw.Content = m.ContentParts
	} else if m.Content != "" {
		raw.Content = m.Content
	}
	return json.Marshal(raw)
}

type OpenAIChatResponse struct {
	ID      string             `json:"id,omitempty"`
	Object  string             `json:"object,omitempty"`
	Created int64              `json:"created,omitempty"`
	Model   string             `json:"model,omitempty"`
	Choices []OpenAIChatChoice `json:"choices"`
	Usage   OpenAIUsage        `json:"usage,omitempty"`
}

type OpenAIChatChoice struct {
	Index        int           `json:"index"`
	Message      OpenAIMessage `json:"message"`
	FinishReason string        `json:"finish_reason,omitempty"`
}

type OpenAIUsage struct {
	PromptTokens     int `json:"prompt_tokens,omitempty"`
	CompletionTokens int `json:"completion_tokens,omitempty"`
	TotalTokens      int `json:"total_tokens,omitempty"`
}
