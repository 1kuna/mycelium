package myce

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDarwinUpgradePreservesCodeIdentity(t *testing.T) {
	for _, change := range []string{"none", "identifier", "requirement", "adhoc", "old-adhoc", "invalid"} {
		t.Run(change, func(t *testing.T) {
			f := newInstallMatrixFixture(t, "darwin", "worker", 2)
			// Only native Mach-O transitions have a macOS code identity. All
			// signing responses are synthetic; no keychain or host tools run.
			old := append([]byte{0xcf, 0xfa, 0xed, 0xfe}, []byte("old executable")...)
			next := append([]byte{0xcf, 0xfa, 0xed, 0xfe}, []byte("new executable")...)
			if err := os.WriteFile(f.c.Binary, old, 0640); err != nil {
				t.Fatal(err)
			}
			f.files[f.c.Binary] = old
			if err := os.WriteFile(f.source, next, 0700); err != nil {
				t.Fatal(err)
			}
			const requirement = `designated => identifier "io.github.1kuna.mycelium" and anchor apple generic and certificate leaf[subject.OU] = "SYNTHETIC"`
			verifiedOld, verifiedCandidate := false, false
			swaps := 0
			run := func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if name != "codesign" {
					return f.manager.run(ctx, name, args...)
				}
				candidate := args[len(args)-1] == f.source
				if args[0] == "--verify" {
					if candidate && change == "invalid" {
						return nil, errors.New("synthetic invalid signature")
					}
					if candidate {
						verifiedCandidate = true
					} else {
						verifiedOld = true
					}
					return nil, nil
				}
				if args[0] == "-dr" {
					text := requirement
					if candidate && change == "requirement" {
						text += ` and certificate leaf[subject.CN] = "DIFFERENT"`
					}
					return []byte(text + "\n"), nil
				}
				id, signature := "io.github.1kuna.mycelium", "Authority=Developer ID Application: synthetic"
				if candidate && change == "identifier" {
					id = "different.app"
				}
				if candidate && change == "adhoc" || !candidate && change == "old-adhoc" {
					signature = "Signature=adhoc"
				}
				return []byte("Identifier=" + id + "\n" + signature + "\nflags=0x10000(runtime)\n"), nil
			}
			files := installFiles{rename: func(source, destination string) error {
				if destination == f.c.Binary {
					if !verifiedOld || !verifiedCandidate || filepath.Dir(source) != filepath.Dir(destination) {
						t.Fatal("binary swap preceded identity verification or crossed directories")
					}
					got, err := os.ReadFile(destination)
					if err != nil || !bytes.Equal(got, old) {
						t.Fatal("old binary moved before atomic replacement")
					}
					swaps++
				}
				return os.Rename(source, destination)
			}}
			err := installWithFiles(context.Background(), f.c, f.source, "fixture", run, files)
			if change != "none" {
				if err == nil || swaps != 0 {
					t.Fatalf("identity change published: %v, swaps=%d", err, swaps)
				}
				f.assertRestored(t)
				return
			}
			if err != nil || swaps != 1 {
				t.Fatalf("stable upgrade failed: %v, swaps=%d", err, swaps)
			}
			c, err := ReadServiceConfig(f.c.Config)
			if err != nil || c.Binary != f.c.Binary || c.Service != f.c.Service {
				t.Fatal("upgrade changed installed path")
			}
			plist, err := os.ReadFile(c.Service)
			if err != nil || !strings.Contains(string(plist), "<key>Label</key><string>local.mycelium.worker</string>") {
				t.Fatal("upgrade changed LaunchAgent label")
			}
		})
	}
}

func TestCodeSigningInspectionCapturesStderr(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	path := filepath.Join(dir, "codesign")
	if err := os.WriteFile(path, []byte("#!/bin/sh\necho synthetic-signature-metadata >&2\n"), 0700); err != nil {
		t.Fatal(err)
	}
	got, err := LocalMachineProbe().Command(context.Background(), path, "-dv", "synthetic-binary")
	if err != nil || string(got) != "synthetic-signature-metadata\n" {
		t.Fatalf("codesign inspection lost stderr: %q, %v", got, err)
	}
}
