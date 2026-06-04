package safeid

import (
	"strings"
	"testing"
)

func TestValidateAcceptsSinglePathComponentIDs(t *testing.T) {
	for _, value := range []string{"tiny", "qwen-9b", "model.gguf", "node_a.1"} {
		if err := Validate("id", value); err != nil {
			t.Fatalf("%q rejected: %v", value, err)
		}
	}
}

func TestValidateRejectsPathLikeIDs(t *testing.T) {
	for _, value := range []string{"", ".", "../x", "x/../y", "/abs", `a\b`, "x..y", " x"} {
		if err := Validate("id", value); err == nil || !strings.Contains(err.Error(), "id") {
			t.Fatalf("%q err = %v", value, err)
		}
	}
}
