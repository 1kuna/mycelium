package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestReleaseVerifiedAssemblySigning(t *testing.T) {
	dir := t.TempDir()
	version, commit := "v3.1.0-test", strings.Repeat("a", 40)
	write := func(path string, b []byte) {
		t.Helper()
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	encode := func(v any) []byte {
		t.Helper()
		b, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		return append(b, '\n')
	}
	source := filepath.Join(dir, "main.go")
	// Running this artifact always fails. Only its real Go metadata may be read.
	write(source, []byte("package main\nfunc main(){ panic(\"candidate must never execute\") }\n"))
	m := myce.ReleaseManifest{Version: version, StartGate: myce.StartGateProtocol, AttemptProtocol: myce.AttemptProtocol}
	builds := map[string]any{}
	for _, target := range myce.ReleaseTargets {
		file := "myce-" + target.OS + "-" + target.Arch
		path := filepath.Join(dir, file)
		cgo := "0"
		if target.NativeSensing {
			cgo = "1"
		}
		cmd := exec.Command("go", "build", "-trimpath", "-buildvcs=false", "-ldflags", "-buildid=myce-release:"+version+":"+commit, "-o", path, source)
		cmd.Env = append(os.Environ(), "GOOS="+target.OS, "GOARCH="+target.Arch, "CGO_ENABLED="+cgo)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("fixture build: %v %s", err, out)
		}
		hash, err := myce.FileSHA256(path)
		if err != nil {
			t.Fatal(err)
		}
		st, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		a := myce.ReleaseArtifact{OS: target.OS, Arch: target.Arch, File: file, SHA256: hash, Size: st.Size(), NativeSensing: target.NativeSensing}
		m.Artifacts = append(m.Artifacts, a)
	}
	for _, kind := range []string{"linux", "darwin"} {
		records := []any{}
		for _, a := range m.Artifacts {
			if a.OS == kind {
				records = append(records, map[string]any{"file": a.File, "sha256": a.SHA256, "size": a.Size})
			}
		}
		builds[kind] = map[string]any{"version": version, "commit": commit, "artifacts": records}
	}
	manifestPath := filepath.Join(dir, "manifest.unsigned.json")
	unsigned := encode(m)
	write(manifestPath, unsigned)
	hash, err := myce.FileSHA256(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	native := map[string]any{"info": myce.VersionInfo{Version: version, Commit: commit, OS: "darwin", Arch: "arm64", StartGate: m.StartGate, AttemptProtocol: m.AttemptProtocol, NativeSensing: true}, "developer_id_verified": true}
	receipt := map[string]any{"version": version, "commit": commit, "builds": builds, "manifest_sha256": hash, "darwin": native}
	receiptPath := filepath.Join(dir, "build-provenance.json")
	write(receiptPath, encode(receipt))
	receiptBytes, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	receiptHash, err := myce.FileSHA256(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(dir, "fake-private.pem")
	write(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	der, err = x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	publicPath := filepath.Join(dir, "fake-public.pem")
	write(publicPath, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
	args := []string{"manifest", "--dir", dir, "--version", version, "--commit", commit, "--assembly-receipt", receiptPath, "--assembly-sha256", receiptHash, "--signing-key", keyPath}
	// Assembly needs Go; the publisher only needs myce. Keep Go unavailable
	// for signing, verify-only and every tampering refusal below.
	t.Setenv("PATH", t.TempDir())
	if _, err := exec.LookPath("go"); err == nil {
		t.Fatal("publisher fixture unexpectedly has Go")
	}
	t.Run("receipt-replacement", func(t *testing.T) {
		// A FIFO can serve pinned bytes to the first read and unrelated proof
		// to a second read. The signer must parse only the first snapshot.
		fifo := filepath.Join(dir, "receipt.fifo")
		if err := syscall.Mkfifo(fifo, 0600); err != nil {
			t.Fatal(err)
		}
		defer os.Remove(fifo)
		pinned := []byte(`{"unrelated":"not assembly proof"}`)
		stop := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			f, err := os.OpenFile(fifo, os.O_WRONLY, 0600)
			if err != nil {
				done <- err
				return
			}
			_, err = f.Write(pinned)
			f.Close()
			if err != nil {
				done <- err
				return
			}
			// The original implementation reopens the FIFO here. Nonblocking
			// writes let the corrected single-read implementation exit cleanly.
			select {
			case <-stop:
				done <- nil
				return
			case <-time.After(50 * time.Millisecond):
			}
			for {
				select {
				case <-stop:
					done <- nil
					return
				default:
				}
				fd, err := syscall.Open(fifo, syscall.O_WRONLY|syscall.O_NONBLOCK, 0600)
				if err == syscall.ENXIO {
					time.Sleep(time.Millisecond)
					continue
				}
				if err != nil {
					done <- err
					return
				}
				file := os.NewFile(uintptr(fd), fifo)
				_, err = file.Write(receiptBytes)
				file.Close()
				done <- err
				return
			}
		}()
		_, err := verifiedAssembly(dir, version, commit, fifo, fmt.Sprintf("%x", sha256.Sum256(pinned)))
		close(stop)
		if writerErr := <-done; writerErr != nil {
			t.Fatal(writerErr)
		}
		if err == nil {
			t.Fatal("signed proof from unauthenticated second receipt read")
		}
	})
	if err := releaseCommand(args); err != nil {
		t.Fatal(err)
	}
	signedPath := filepath.Join(dir, "release.json")
	signed, err := os.ReadFile(signedPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := myce.VerifyRelease(signed, pub); err != nil {
		t.Fatal("newline assembly signature refused", err)
	}
	approve := []string{"approve", "--verify-only", "--config", filepath.Join(dir, "does-not-exist"), "--release-manifest", signedPath, "--release-key", publicPath, "--release-url", "https://publisher.invalid/release"}
	before, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := map[string][]byte{}
	for _, e := range before {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		snapshot[e.Name()] = b
	}
	captureJoiningOutput(t, func() error { return releaseCommand(approve) })
	after, err := os.ReadDir(dir)
	if err != nil || len(after) != len(before) {
		t.Fatal("verify-only created files", err)
	}
	for name, b := range snapshot {
		actual, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || !bytes.Equal(b, actual) {
			t.Fatal("verify-only wrote", name, err)
		}
	}
	for _, mode := range []string{"signature", "matrix", "trust", "ca"} {
		t.Run("verify-"+mode, func(t *testing.T) {
			badArgs := append([]string{}, approve...)
			switch mode {
			case "signature":
				write(signedPath, bytes.Replace(signed, []byte(version), []byte("v3.9.0-test"), 1))
				defer write(signedPath, signed)
			case "matrix":
				bad := m
				bad.Artifacts = nil
				payload, _ := json.Marshal(bad)
				b, _ := json.Marshal(myce.SignedRelease{Manifest: payload, Signature: ed25519.Sign(private, payload)})
				write(signedPath, b)
				defer write(signedPath, signed)
			case "trust":
				badArgs[len(badArgs)-1] = "http://publisher.invalid/release"
			case "ca":
				caPath := filepath.Join(dir, "bad-ca.pem")
				write(caPath, []byte("invalid"))
				badArgs = append(badArgs, "--release-ca", caPath)
			}
			if err := releaseCommand(badArgs); err == nil {
				t.Fatal("bad release accepted")
			}
		})
	}
	for _, mode := range []string{"receipt", "manifest", "artifact", "native-proof", "source", "build-receipt", "target"} {
		t.Run(mode, func(t *testing.T) {
			write(receiptPath, receiptBytes)
			write(manifestPath, unsigned)
			defer write(receiptPath, receiptBytes)
			defer write(manifestPath, unsigned)
			badArgs := append([]string{}, args...)
			switch mode {
			case "receipt":
				write(receiptPath, append(receiptBytes, ' '))
			case "manifest":
				write(manifestPath, append(unsigned, ' '))
			case "artifact":
				path := filepath.Join(dir, m.Artifacts[0].File)
				original, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				changed := append([]byte{}, original...)
				changed[len(changed)-1] ^= 1
				write(path, changed)
				defer write(path, original)
			case "source":
				badArgs[6] = strings.Repeat("b", 40)
			default:
				var changed map[string]any
				if err := json.Unmarshal(receiptBytes, &changed); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "native-proof":
					changed["darwin"].(map[string]any)["developer_id_verified"] = false
				case "build-receipt":
					changed["builds"].(map[string]any)["linux"].(map[string]any)["commit"] = strings.Repeat("b", 40)
				case "target":
					info := changed["darwin"].(map[string]any)["info"].(map[string]any)
					info["os"] = "linux"
				}
				write(receiptPath, encode(changed))
				hash, err := myce.FileSHA256(receiptPath)
				if err != nil {
					t.Fatal(err)
				}
				badArgs[10] = hash
			}
			if err := releaseCommand(badArgs); err == nil {
				t.Fatal("mismatched assembly accepted")
			}
		})
	}
}
