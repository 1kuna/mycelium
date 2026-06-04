package translate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"mycelium/internal/domain"
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
	Kind            IngressKind
	Model           string
	Stream          bool
	Project         string
	Priority        domain.Priority
	SpeedPref       domain.SpeedPref
	ContextRequest  int
	Preemption      domain.Preemption
	ConversationKey string
	Handling        domain.HandlingClass
	Submitter       string
	Body            []byte
	OpenAI          api.OpenAIChatRequest
	Complete        api.OpenAICompletionRequest
	Claude          api.AnthropicMessagesRequest
	typedErr        error
}

type UpstreamRequest struct {
	Path      string
	Body      []byte
	Headers   map[string]string
	Translate bool
}

func ParseOpenAIChat(body []byte) (IngressRequest, error) {
	var basic struct {
		Model    string            `json:"model"`
		Stream   bool              `json:"stream"`
		Messages []json.RawMessage `json:"messages"`
	}
	if err := decodeJSON(body, &basic); err != nil {
		return IngressRequest{}, err
	}
	if len(basic.Messages) == 0 {
		return IngressRequest{}, fmt.Errorf("messages are required")
	}
	var req api.OpenAIChatRequest
	typedErr := decodeStrict(body, &req)
	return IngressRequest{Kind: KindOpenAIChat, Model: basic.Model, Stream: basic.Stream, Body: append([]byte(nil), body...), OpenAI: req, typedErr: typedErr}, nil
}

func ParseOpenAICompletion(body []byte) (IngressRequest, error) {
	var basic struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if err := decodeJSON(body, &basic); err != nil {
		return IngressRequest{}, err
	}
	var req api.OpenAICompletionRequest
	typedErr := decodeStrict(body, &req)
	return IngressRequest{Kind: KindOpenAICompletion, Model: basic.Model, Stream: basic.Stream, Body: append([]byte(nil), body...), Complete: req, typedErr: typedErr}, nil
}

func ParseAnthropicMessages(body []byte) (IngressRequest, error) {
	var basic struct {
		Model     string            `json:"model"`
		MaxTokens int               `json:"max_tokens"`
		Stream    bool              `json:"stream"`
		Messages  []json.RawMessage `json:"messages"`
	}
	if err := decodeJSON(body, &basic); err != nil {
		return IngressRequest{}, err
	}
	if basic.MaxTokens == 0 {
		return IngressRequest{}, fmt.Errorf("max_tokens is required")
	}
	if len(basic.Messages) == 0 {
		return IngressRequest{}, fmt.Errorf("messages are required")
	}
	var req api.AnthropicMessagesRequest
	typedErr := decodeStrict(body, &req)
	return IngressRequest{Kind: KindAnthropicMessages, Model: basic.Model, Stream: basic.Stream, Body: append([]byte(nil), body...), Claude: req, typedErr: typedErr}, nil
}

func WithModel(req IngressRequest, model string) (IngressRequest, error) {
	if model == "" {
		return IngressRequest{}, fmt.Errorf("model is required")
	}
	req.Model = model
	var err error
	body := req.Body
	if len(body) > 0 {
		body, err = setJSONModel(body, model)
		if err != nil {
			return IngressRequest{}, err
		}
	}
	switch req.Kind {
	case KindOpenAIChat:
		req.OpenAI.Model = model
		if len(body) == 0 {
			body, err = json.Marshal(req.OpenAI)
		}
	case KindOpenAICompletion:
		req.Complete.Model = model
		if len(body) == 0 {
			body, err = json.Marshal(req.Complete)
		}
	case KindAnthropicMessages:
		req.Claude.Model = model
		if len(body) == 0 {
			body, err = json.Marshal(req.Claude)
		}
	default:
		return IngressRequest{}, fmt.Errorf("unsupported ingress kind %q", req.Kind)
	}
	if err != nil {
		return IngressRequest{}, err
	}
	req.Body = body
	return req, nil
}

func BuildUpstream(req IngressRequest, profile profiles.Profile) (UpstreamRequest, error) {
	if err := validateProfile(req, profile); err != nil {
		return UpstreamRequest{}, err
	}
	headers := profileHeaders(profile)
	switch req.Kind {
	case KindOpenAIChat:
		if profile.Format == profiles.FormatOpenAI {
			return UpstreamRequest{Path: profile.ChatPath, Body: req.Body, Headers: headers}, nil
		}
		if profile.Format != profiles.FormatAnthropic {
			return UpstreamRequest{}, fmt.Errorf("openai chat to %s translation is not supported", profile.Format)
		}
		if req.OpenAI.Stream {
			return UpstreamRequest{}, fmt.Errorf("streaming openai-to-anthropic translation is not supported")
		}
		if req.typedErr != nil {
			return UpstreamRequest{}, fmt.Errorf("openai chat request cannot be translated without protocol loss: %w", req.typedErr)
		}
		claude, err := openAIChatToAnthropic(req.OpenAI)
		if err != nil {
			return UpstreamRequest{}, err
		}
		body, err := json.Marshal(claude)
		if err != nil {
			return UpstreamRequest{}, err
		}
		return UpstreamRequest{Path: profile.AnthropicPath, Body: body, Headers: headers, Translate: true}, nil
	case KindOpenAICompletion:
		if profile.Format == profiles.FormatOpenAI {
			return UpstreamRequest{Path: profile.CompletionPath, Body: req.Body, Headers: headers}, nil
		}
		if profile.Format != profiles.FormatAnthropic {
			return UpstreamRequest{}, fmt.Errorf("openai completion to %s translation is not supported", profile.Format)
		}
		if req.Complete.Stream {
			return UpstreamRequest{}, fmt.Errorf("streaming openai-completion-to-anthropic translation is not supported")
		}
		if req.typedErr != nil {
			return UpstreamRequest{}, fmt.Errorf("openai completion request cannot be translated without protocol loss: %w", req.typedErr)
		}
		claude, err := openAICompletionToAnthropic(req.Complete)
		if err != nil {
			return UpstreamRequest{}, err
		}
		body, err := json.Marshal(claude)
		if err != nil {
			return UpstreamRequest{}, err
		}
		return UpstreamRequest{Path: profile.AnthropicPath, Body: body, Headers: headers, Translate: true}, nil
	case KindAnthropicMessages:
		if profile.Format == profiles.FormatAnthropic {
			return UpstreamRequest{Path: profile.AnthropicPath, Body: req.Body, Headers: headers}, nil
		}
		if profile.Format != profiles.FormatOpenAI {
			return UpstreamRequest{}, fmt.Errorf("anthropic messages to %s translation is not supported", profile.Format)
		}
		if req.Claude.Stream {
			return UpstreamRequest{}, fmt.Errorf("streaming anthropic-to-openai translation is not supported")
		}
		if req.typedErr != nil {
			return UpstreamRequest{}, fmt.Errorf("anthropic messages request cannot be translated without protocol loss: %w", req.typedErr)
		}
		openai, err := anthropicToOpenAI(req.Claude)
		if err != nil {
			return UpstreamRequest{}, err
		}
		body, err := json.Marshal(openai)
		if err != nil {
			return UpstreamRequest{}, err
		}
		return UpstreamRequest{Path: profile.ChatPath, Body: body, Headers: headers, Translate: true}, nil
	default:
		return UpstreamRequest{}, fmt.Errorf("unsupported ingress kind %q", req.Kind)
	}
}

func validateProfile(req IngressRequest, profile profiles.Profile) error {
	if profile.ID != "" {
		if !profile.SupportsChat && (req.Kind == KindOpenAIChat || req.Kind == KindAnthropicMessages) {
			return fmt.Errorf("provider profile %q does not support chat/messages", profile.ID)
		}
		if req.Stream && !profile.SupportsStream {
			return fmt.Errorf("provider profile %q does not support streaming", profile.ID)
		}
	}
	switch profile.Format {
	case profiles.FormatOpenAI:
		switch req.Kind {
		case KindOpenAIChat, KindAnthropicMessages:
			if profile.ChatPath == "" {
				return fmt.Errorf("provider profile %q has no chat path", profile.ID)
			}
		case KindOpenAICompletion:
			if profile.CompletionPath == "" {
				return fmt.Errorf("provider profile %q has no completion path", profile.ID)
			}
		}
	case profiles.FormatAnthropic:
		if profile.AnthropicPath == "" {
			return fmt.Errorf("provider profile %q has no anthropic messages path", profile.ID)
		}
	default:
		return nil
	}
	return nil
}

func profileHeaders(profile profiles.Profile) map[string]string {
	if len(profile.Headers) == 0 {
		return nil
	}
	headers := make(map[string]string, len(profile.Headers))
	for key, value := range profile.Headers {
		headers[key] = value
	}
	return headers
}

func TranslateResponse(req IngressRequest, upstream UpstreamRequest, body []byte) ([]byte, string, error) {
	if !upstream.Translate {
		return body, "application/json", nil
	}
	switch req.Kind {
	case KindOpenAIChat:
		var claude api.AnthropicMessagesResponse
		if err := json.Unmarshal(body, &claude); err != nil {
			return nil, "", err
		}
		out, err := anthropicToOpenAIChatResponse(claude)
		if err != nil {
			return nil, "", err
		}
		data, err := json.Marshal(out)
		if err != nil {
			return nil, "", err
		}
		return data, "application/json", nil
	case KindOpenAICompletion:
		var claude api.AnthropicMessagesResponse
		if err := json.Unmarshal(body, &claude); err != nil {
			return nil, "", err
		}
		out, err := anthropicToOpenAICompletionResponse(claude)
		if err != nil {
			return nil, "", err
		}
		data, err := json.Marshal(out)
		if err != nil {
			return nil, "", err
		}
		return data, "application/json", nil
	case KindAnthropicMessages:
		var openai api.OpenAIChatResponse
		if err := json.Unmarshal(body, &openai); err != nil {
			return nil, "", err
		}
		out, err := openAIToAnthropic(openai)
		if err != nil {
			return nil, "", err
		}
		data, err := json.Marshal(out)
		if err != nil {
			return nil, "", err
		}
		return data, "application/json", nil
	default:
		return nil, "", fmt.Errorf("unsupported translated response kind %q", req.Kind)
	}
}

func anthropicToOpenAI(req api.AnthropicMessagesRequest) (api.OpenAIChatRequest, error) {
	if len(req.Tools) > 0 || req.ToolChoice != nil {
		return api.OpenAIChatRequest{}, fmt.Errorf("anthropic tool use cannot be translated to openai-compatible backends without protocol loss")
	}
	messages := make([]api.OpenAIMessage, 0, len(req.Messages)+1)
	if req.System != "" {
		messages = append(messages, api.OpenAIMessage{Role: "system", Content: req.System})
	}
	for _, msg := range req.Messages {
		text, err := anthropicText(msg.Content)
		if err != nil {
			return api.OpenAIChatRequest{}, err
		}
		messages = append(messages, api.OpenAIMessage{Role: msg.Role, Content: text})
	}
	return api.OpenAIChatRequest{
		Model:     req.Model,
		Messages:  messages,
		MaxTokens: req.MaxTokens,
		Stream:    req.Stream,
	}, nil
}

func openAIChatToAnthropic(req api.OpenAIChatRequest) (api.AnthropicMessagesRequest, error) {
	if req.MaxTokens <= 0 {
		return api.AnthropicMessagesRequest{}, fmt.Errorf("openai chat max_tokens is required for anthropic translation")
	}
	if req.Stream {
		return api.AnthropicMessagesRequest{}, fmt.Errorf("streaming openai-to-anthropic translation is not supported")
	}
	if req.Temperature != nil {
		return api.AnthropicMessagesRequest{}, fmt.Errorf("openai temperature cannot be translated to anthropic without explicit support")
	}
	if len(req.Tools) > 0 || len(req.ToolChoice) > 0 {
		return api.AnthropicMessagesRequest{}, fmt.Errorf("openai tool use cannot be translated to anthropic-compatible backends without protocol loss")
	}
	var system []string
	messages := make([]api.AnthropicMessage, 0, len(req.Messages))
	for _, msg := range req.Messages {
		text, err := openAIMessageText(msg)
		if err != nil {
			return api.AnthropicMessagesRequest{}, err
		}
		switch msg.Role {
		case "system":
			system = append(system, text)
		case "user", "assistant":
			messages = append(messages, api.AnthropicMessage{Role: msg.Role, Content: []api.AnthropicContentBlock{{Type: "text", Text: text}}})
		default:
			return api.AnthropicMessagesRequest{}, fmt.Errorf("openai role %q cannot be translated to anthropic messages", msg.Role)
		}
	}
	if len(messages) == 0 {
		return api.AnthropicMessagesRequest{}, fmt.Errorf("openai chat translation requires at least one user or assistant message")
	}
	return api.AnthropicMessagesRequest{
		Model:     req.Model,
		System:    strings.Join(system, "\n"),
		Messages:  messages,
		MaxTokens: req.MaxTokens,
	}, nil
}

func openAICompletionToAnthropic(req api.OpenAICompletionRequest) (api.AnthropicMessagesRequest, error) {
	if req.MaxTokens <= 0 {
		return api.AnthropicMessagesRequest{}, fmt.Errorf("openai completion max_tokens is required for anthropic translation")
	}
	if req.Stream {
		return api.AnthropicMessagesRequest{}, fmt.Errorf("streaming openai-completion-to-anthropic translation is not supported")
	}
	if req.Prompt == "" {
		return api.AnthropicMessagesRequest{}, fmt.Errorf("openai completion prompt is required for anthropic translation")
	}
	return api.AnthropicMessagesRequest{
		Model:     req.Model,
		MaxTokens: req.MaxTokens,
		Messages: []api.AnthropicMessage{{
			Role:    "user",
			Content: []api.AnthropicContentBlock{{Type: "text", Text: req.Prompt}},
		}},
	}, nil
}

func openAIMessageText(msg api.OpenAIMessage) (string, error) {
	if msg.Name != "" || msg.ToolCallID != "" || len(msg.ToolCalls) > 0 || msg.FunctionCall != nil {
		return "", fmt.Errorf("openai message role %q contains tool/function fields that cannot be translated to anthropic", msg.Role)
	}
	if len(msg.ContentParts) == 0 {
		return msg.Content, nil
	}
	parts := make([]string, 0, len(msg.ContentParts))
	for _, part := range msg.ContentParts {
		if part.Type != "text" {
			return "", fmt.Errorf("openai content part %q cannot be translated to anthropic text", part.Type)
		}
		parts = append(parts, part.Text)
	}
	return strings.Join(parts, "\n"), nil
}

func anthropicToOpenAIChatResponse(resp api.AnthropicMessagesResponse) (api.OpenAIChatResponse, error) {
	text, err := anthropicResponseText(resp)
	if err != nil {
		return api.OpenAIChatResponse{}, err
	}
	finishReason, err := anthropicStopReasonToOpenAI(resp.StopReason)
	if err != nil {
		return api.OpenAIChatResponse{}, err
	}
	return api.OpenAIChatResponse{
		ID:    resp.ID,
		Model: resp.Model,
		Choices: []api.OpenAIChatChoice{{
			Index:        0,
			Message:      api.OpenAIMessage{Role: "assistant", Content: text},
			FinishReason: finishReason,
		}},
		Usage: api.OpenAIUsage{
			PromptTokens:     resp.Usage.InputTokens,
			CompletionTokens: resp.Usage.OutputTokens,
			TotalTokens:      resp.Usage.InputTokens + resp.Usage.OutputTokens,
		},
	}, nil
}

func anthropicToOpenAICompletionResponse(resp api.AnthropicMessagesResponse) (api.OpenAICompletionResponse, error) {
	text, err := anthropicResponseText(resp)
	if err != nil {
		return api.OpenAICompletionResponse{}, err
	}
	finishReason, err := anthropicStopReasonToOpenAI(resp.StopReason)
	if err != nil {
		return api.OpenAICompletionResponse{}, err
	}
	return api.OpenAICompletionResponse{
		ID:    resp.ID,
		Model: resp.Model,
		Choices: []api.OpenAICompletionChoice{{
			Index:        0,
			Text:         text,
			FinishReason: finishReason,
		}},
		Usage: api.OpenAIUsage{
			PromptTokens:     resp.Usage.InputTokens,
			CompletionTokens: resp.Usage.OutputTokens,
			TotalTokens:      resp.Usage.InputTokens + resp.Usage.OutputTokens,
		},
	}, nil
}

func anthropicResponseText(resp api.AnthropicMessagesResponse) (string, error) {
	if resp.Role != "" && resp.Role != "assistant" {
		return "", fmt.Errorf("anthropic response role %q cannot be translated to openai assistant message", resp.Role)
	}
	parts := make([]string, 0, len(resp.Content))
	for _, block := range resp.Content {
		if block.Type != "text" {
			return "", fmt.Errorf("anthropic response content block %q cannot be translated to openai text", block.Type)
		}
		parts = append(parts, block.Text)
	}
	return strings.Join(parts, "\n"), nil
}

func openAIToAnthropic(resp api.OpenAIChatResponse) (api.AnthropicMessagesResponse, error) {
	if len(resp.Choices) == 0 {
		return api.AnthropicMessagesResponse{}, fmt.Errorf("openai response has no choices to translate to anthropic")
	}
	if len(resp.Choices) > 1 {
		return api.AnthropicMessagesResponse{}, fmt.Errorf("openai response has %d choices; multi-choice translation to anthropic is unsupported", len(resp.Choices))
	}
	choice := resp.Choices[0]
	msg := choice.Message
	if msg.Role != "" && msg.Role != "assistant" {
		return api.AnthropicMessagesResponse{}, fmt.Errorf("openai response role %q cannot be translated to anthropic assistant message", msg.Role)
	}
	if len(msg.UnsupportedResponseFields) > 0 {
		return api.AnthropicMessagesResponse{}, fmt.Errorf("openai response contains unsupported fields that cannot be translated to anthropic: %s", strings.Join(msg.UnsupportedResponseFields, ","))
	}
	text, err := openAIMessageText(msg)
	if err != nil {
		return api.AnthropicMessagesResponse{}, err
	}
	stopReason, err := openAIStopReasonToAnthropic(choice.FinishReason)
	if err != nil {
		return api.AnthropicMessagesResponse{}, err
	}
	return api.AnthropicMessagesResponse{
		ID:         resp.ID,
		Type:       "message",
		Role:       "assistant",
		Model:      resp.Model,
		Content:    []api.AnthropicContentBlock{{Type: "text", Text: text}},
		StopReason: stopReason,
		Usage: api.AnthropicUsage{
			InputTokens:  resp.Usage.PromptTokens,
			OutputTokens: resp.Usage.CompletionTokens,
		},
	}, nil
}

func anthropicText(blocks []api.AnthropicContentBlock) (string, error) {
	parts := make([]string, 0, len(blocks))
	for _, block := range blocks {
		if block.Type != "text" {
			return "", fmt.Errorf("anthropic content block %q cannot be translated to openai text", block.Type)
		}
		parts = append(parts, block.Text)
	}
	return strings.Join(parts, "\n"), nil
}

func openAIStopReasonToAnthropic(reason string) (string, error) {
	switch reason {
	case "":
		return "", nil
	case "stop":
		return "end_turn", nil
	case "length":
		return "max_tokens", nil
	case "tool_calls", "function_call":
		return "", fmt.Errorf("openai finish_reason %q cannot be translated to anthropic without tool-call loss", reason)
	case "content_filter":
		return "", fmt.Errorf("openai finish_reason %q cannot be translated to anthropic without content-filter loss", reason)
	default:
		return "", fmt.Errorf("openai finish_reason %q is unsupported for anthropic translation", reason)
	}
}

func anthropicStopReasonToOpenAI(reason string) (string, error) {
	switch reason {
	case "":
		return "", nil
	case "end_turn", "stop_sequence":
		return "stop", nil
	case "max_tokens":
		return "length", nil
	case "tool_use":
		return "", fmt.Errorf("anthropic stop_reason %q cannot be translated to openai without tool-call loss", reason)
	case "pause_turn", "refusal":
		return "", fmt.Errorf("anthropic stop_reason %q cannot be translated to openai without lifecycle loss", reason)
	default:
		return "", fmt.Errorf("anthropic stop_reason %q is unsupported for openai translation", reason)
	}
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

func decodeJSON(body []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("request body contains multiple JSON values")
	}
	return nil
}

func setJSONModel(body []byte, model string) ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := decodeJSON(body, &fields); err != nil {
		return nil, err
	}
	modelRaw, err := json.Marshal(model)
	if err != nil {
		return nil, err
	}
	fields["model"] = modelRaw
	return json.Marshal(fields)
}
