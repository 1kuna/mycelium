// statusdoc updates only the generated field inventory in docs/status.md.
package main

import (
	"bytes"
	"flag"
	"log"
	"os"
	"path/filepath"

	"mycelium/internal/statusdoc"
)

func main() {
	root := flag.String("root", ".", "repository root")
	flag.Parse()
	path := filepath.Join(*root, "docs/status.md")
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	start := bytes.Index(data, []byte("<!-- BEGIN GENERATED STATUS FIELDS -->"))
	endMarker := []byte("<!-- END GENERATED STATUS FIELDS -->")
	end := bytes.Index(data, endMarker)
	if start < 0 || end < start {
		log.Fatal("missing status inventory markers")
	}
	result := append(append(append([]byte{}, data[:start]...), []byte(statusdoc.Fields())...), data[end+len(endMarker):]...)
	if err := os.WriteFile(path, result, 0644); err != nil {
		log.Fatal(err)
	}
}
