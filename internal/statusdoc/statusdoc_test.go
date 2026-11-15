package statusdoc

import (
	"os"
	"strings"
	"testing"
)

func TestStatusFieldsCurrent(t *testing.T) {
	data, err := os.ReadFile("../../docs/status.md")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(data), "<!-- BEGIN GENERATED STATUS FIELDS -->")
	endMarker := "<!-- END GENERATED STATUS FIELDS -->"
	end := strings.Index(string(data), endMarker)
	if start < 0 || end < start {
		t.Fatal("missing generated status inventory")
	}
	if string(data)[start:end+len(endMarker)] != Fields() {
		t.Fatal("status fields drifted from Go types; run go generate ./internal/statusdoc")
	}
}
