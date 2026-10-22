//go:build darwin && cgo

package main

import (
	"bytes"
	"debug/macho"
	"encoding/xml"
	"os"
	"strings"
	"testing"
)

func TestDarwinEmbeddedLocalNetworkMetadata(t *testing.T) {
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	f, err := macho.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s := f.Section("__info_plist")
	if s == nil || s.Seg != "__TEXT" {
		t.Fatal("Darwin binary lacks embedded Local Network permission metadata")
	}
	b, err := s.Data()
	if err != nil {
		t.Fatal(err)
	}
	var plist struct {
		Dict struct {
			Inner string `xml:",innerxml"`
		} `xml:"dict"`
	}
	if err := xml.Unmarshal(bytes.TrimRight(b, "\x00"), &plist); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"CFBundleIdentifier", "io.github.1kuna.mycelium", "CFBundleName", "Mycelium", "NSLocalNetworkUsageDescription", "NSBonjourServices", "_mycelium._tcp"} {
		if !strings.Contains(plist.Dict.Inner, value) {
			t.Fatal("embedded permission metadata missing", value)
		}
	}
}
