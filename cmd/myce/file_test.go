package main

import (
	"os"
	"path/filepath"
	"testing"

	"mycelium/internal/myce"
)

func TestFileDestinationIdentityCommand(t *testing.T) {
	root := t.TempDir()
	config := filepath.Join(t.TempDir(), "destinations.json")
	f, err := os.Create(config)
	if err != nil {
		t.Fatal(err)
	}
	previous := os.Stdout
	os.Stdout = f
	err = fileCommand([]string{"destination-identity", "--destination-id", "archive", root})
	os.Stdout = previous
	if closeErr := f.Close(); err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	roots, err := myce.LoadOutputDestinations(config)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := myce.ObserveStorageBackingIdentity(root)
	if err != nil || roots["archive"].NativeIdentity != expected || roots["archive"].Path != root {
		t.Fatal("identity configuration did not round-trip", roots, err)
	}
}
