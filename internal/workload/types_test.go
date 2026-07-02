package workload

import (
	"strings"
	"testing"
)

func TestDecodeExecutablePayload(t *testing.T) {
	for _, kind := range []string{
		ExecutablePayloadKindOpenAIChat,
		ExecutablePayloadKindOpenAICompletion,
		ExecutablePayloadKindAnthropicMessages,
	} {
		payload := []byte(`{"kind":"` + kind + `","body":{"model":"model-a"}}`)
		got, err := DecodeExecutablePayload(payload)
		if err != nil {
			t.Fatalf("%s DecodeExecutablePayload: %v", kind, err)
		}
		if got.Kind != kind || string(got.Body) == "" {
			t.Fatalf("%s decoded executable payload = %+v", kind, got)
		}
	}
}

func TestDecodeExecutablePayloadRejectsNonExecutableInputs(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    string
	}{
		{name: "empty", payload: ``, want: "empty"},
		{name: "malformed", payload: `{`, want: "unexpected"},
		{name: "opaque", payload: `{"input_bytes":123}`, want: "unsupported"},
		{name: "bad kind", payload: `{"kind":"raw","body":{}}`, want: "unsupported"},
		{name: "missing body", payload: `{"kind":"openai_chat"}`, want: "body"},
		{name: "non object body", payload: `{"kind":"openai_chat","body":[]}`, want: "JSON object"},
	}
	for _, tc := range tests {
		_, err := DecodeExecutablePayload([]byte(tc.payload))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s err = %v, want %q", tc.name, err, tc.want)
		}
	}
}
