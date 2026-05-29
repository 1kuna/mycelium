package translate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"mycelium/internal/gateway/profiles"
	"mycelium/pkg/api"
)

type IngressKind string

const (
	KindOpenAIChat        IngressKind = "openai_chat"
	KindOpenAICompletion  IngressKind = "openai_completion"
	KindAnthropicMessages IngressKind = "anthropic_messages"
)

type IngressRequest struct {
	Kind     IngressKind
	Model    string
	Stream   bool
	Body     []byte
	OpenAI   api.OpenAIChatRequest
	Complete api.OpenAICompletionRequest
	Claude   api.AnthropicMessagesRequest
}

type UpstreamRequest struct {
	Path      string
	Body      []byte
	Translate bool
}

func ParseOpenAIChat(body []byte) (IngressRequest, error) {
	var req api.OpenAIChatRequest
	if err := decodeStrict(body, &req); err != nil {
		return IngressRequest{}, err
	}
	if req.Model == "" {
		return IngressRequest{}, fmt.Errorf("model is required")
	}
	if len(req.Messages) == 0 {
		return IngressRequest{}, fmt.Errorf("messages are required")
	}
	return IngressRequest{Kind: KindOpenAIChat, Model: req.Model, Stream: req.Stream, Body: append([]byte(nil), body...), OpenAI: req}, nil
}

func ParseOpenAICompletion(body []byte) (IngressRequest, error) {
	var req api.OpenAICompletionRequest
	if err := decodeStrict(body, &req); err != nil {
		return IngressRequest{}, err
	}
	if req.Model == "" {
		return IngressRequest{}, fmt.Errorf("model is required")
	}
	return IngressRequest{Kind: KindOpenAICompletion, Model: req.Model, Stream: req.Stream, Body: append([]byte(nil), body...), Complete: req}, nil
}

func ParseAnthropicMessages(body []byte) (IngressRequest, error) {
	var req api.AnthropicMessagesRequest
	if err := decodeStrict(body, &req); err != nil {
		return IngressRequest{}, err
	}
	if req.Model == "" {
		return IngressRequest{}, fmt.Errorf("model is required")
	}
	if req.MaxTokens == 0 {
		return IngressRequest{}, fmt.Errorf("max_tokens is required")
	}
	if len(req.Messages) == 0 {
		return IngressRequest{}, fmt.Errorf("messages are required")
	}
	return IngressRequest{Kind: KindAnthropicMessages, Model: req.Model, Stream: req.Stream, Body: append([]byte(nil), body...), Claude: req}, nil
}

func BuildUpstream(req IngressRequest, profile profiles.Profile) (UpstreamRequest, error) {
	switch req.Kind {
	case KindOpenAIChat:
		if profile.Format != profiles.FormatOpenAI {
			return UpstreamRequest{}, fmt.Errorf("openai chat to %s translation is not supported", profile.Format)
		}
		return UpstreamRequest{Path: profile.ChatPath, Body: req.Body}, nil
	case KindOpenAICompletion:
		if profile.Format != profiles.FormatOpenAI {
			return UpstreamRequest{}, fmt.Errorf("openai completion to %s translation is not supported", profile.Format)
		}
		return UpstreamRequest{Path: profile.CompletionPath, Body: req.Body}, nil
	case KindAnthropicMessages:
		if profile.Format == profiles.FormatAnthropic {
			return UpstreamRequest{Path: profile.AnthropicPath, Body: req.Body}, nil
		}
		if profile.Format != profiles.FormatOpenAI {
			return UpstreamRequest{}, fmt.Errorf("anthropic messages to %s translation is not supported", profile.Format)
		}
		if req.Claude.Stream {
			return UpstreamRequest{}, fmt.Errorf("streaming anthropic-to-openai translation is not supported")
		}
		body, err := json.Marshal(anthropicToOpenAI(req.Claude))
		if err != nil {
			return UpstreamRequest{}, err
		}
		return UpstreamRequest{Path: profile.ChatPath, Body: body, Translate: true}, nil
	default:
		return UpstreamRequest{}, fmt.Errorf("unsupported ingress kind %q", req.Kind)
	}
}

func TranslateResponse(req IngressRequest, upstream UpstreamRequest, body []byte) ([]byte, string, error) {
	if !upstream.Translate {
		return body, "application/json", nil
	}
	switch req.Kind {
	case KindAnthropicMessages:
		var openai api.OpenAIChatResponse
		if err := json.Unmarshal(body, &openai); err != nil {
			return nil, "", err
		}
		out := openAIToAnthropic(openai)
		data, err := json.Marshal(out)
		if err != nil {
			return nil, "", err
		}
		return data, "application/json", nil
	default:
		return nil, "", fmt.Errorf("unsupported translated response kind %q", req.Kind)
	}
}

func anthropicToOpenAI(req api.AnthropicMessagesRequest) api.OpenAIChatRequest {
	messages := make([]api.OpenAIMessage, 0, len(req.Messages)+1)
	if req.System != "" {
		messages = append(messages, api.OpenAIMessage{Role: "system", Content: req.System})
	}
	for _, msg := range req.Messages {
		messages = append(messages, api.OpenAIMessage{Role: msg.Role, Content: anthropicText(msg.Content)})
	}
	return api.OpenAIChatRequest{
		Model:     req.Model,
		Messages:  messages,
		MaxTokens: req.MaxTokens,
		Stream:    req.Stream,
	}
}

func openAIToAnthropic(resp api.OpenAIChatResponse) api.AnthropicMessagesResponse {
	text := ""
	stop := ""
	if len(resp.Choices) > 0 {
		text = resp.Choices[0].Message.Content
		stop = resp.Choices[0].FinishReason
	}
	return api.AnthropicMessagesResponse{
		ID:         resp.ID,
		Type:       "message",
		Role:       "assistant",
		Model:      resp.Model,
		Content:    []api.AnthropicContentBlock{{Type: "text", Text: text}},
		StopReason: stop,
		Usage: api.AnthropicUsage{
			InputTokens:  resp.Usage.PromptTokens,
			OutputTokens: resp.Usage.CompletionTokens,
		},
	}
}

func anthropicText(blocks []api.AnthropicContentBlock) string {
	parts := make([]string, 0, len(blocks))
	for _, block := range blocks {
		if block.Type == "text" {
			parts = append(parts, block.Text)
		}
	}
	return strings.Join(parts, "\n")
}

func decodeStrict(body []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("request body contains multiple JSON values")
	}
	return nil
}
