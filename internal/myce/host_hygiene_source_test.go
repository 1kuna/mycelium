package myce

import (
	"slices"
	"strings"
	"testing"
)

func TestHostHygieneForeignConsumersExcludeOwnedTree(t *testing.T) {
	data := []byte("10 1 3.0 100 /usr/bin/owner\n11 10 99.0 100 /tmp/child\n12 11 99.0 100 /tmp/grandchild\n20 1 50.0 200 /usr/bin/foreign\n")
	out := foreignHealthProcesses(data, map[int]bool{10: true})
	if len(out) != 1 || out[0].PID != 20 || out[0].Executable != "foreign" || out[0].RSSBytes != 200*1024 {
		t.Fatal(out)
	}
	if slices.ContainsFunc(out, func(p ForeignConsumer) bool { return strings.Contains(p.Executable, "/") }) {
		t.Fatal("published executable paths")
	}
}
