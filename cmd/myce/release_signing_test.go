package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// All tools, including codesign, are fake. This test never opens a keychain.
func TestReleaseSignsDarwinBeforeManifest(t *testing.T) {
	for _, mode := range []string{"configured", "verify-fails"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			log := filepath.Join(dir, "calls")
			tools := map[string]string{
				"uname": `case "$1" in -s) echo Darwin;; -m) echo arm64;; esac`,
				"git": `test "$*" = 'rev-parse HEAD'
echo 0123456789abcdef0123456789abcdef01234567`,
				"go": `echo build >> "$TEST_SIGN_LOG"
while test "$#" -gt 0; do if test "$1" = -o; then shift; out=$1; fi; shift; done
cat > "$out" <<'BIN'
#!/bin/sh
case "$1:$2" in
release:build-targets) printf 'darwin arm64 1\nlinux amd64 0\nlinux arm64 0\n';;
release:manifest) echo manifest >> "$TEST_SIGN_LOG";;
esac
BIN
chmod +x "$out"`,
				"codesign": `echo "codesign $*" >> "$TEST_SIGN_LOG"
test "$TEST_SIGN_MODE" != sign-fails || exit 42
if test "$TEST_SIGN_MODE" = verify-fails && test "$1" = --verify; then exit 43; fi`,
			}
			for name, text := range tools {
				if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\nset -eu\n"+text+"\n"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command("sh", "../../scripts/release.sh", "v3.1.0-test", filepath.Join(dir, "release"), filepath.Join(dir, "fake-key"))
			cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "TEST_SIGN_LOG="+log, "TEST_SIGN_MODE="+mode, "MYCE_DEVELOPER_ID=")
			if mode != "unsigned" {
				cmd.Env = append(cmd.Env, "MYCE_DEVELOPER_ID=fake-test-identity")
			}
			out, err := cmd.CombinedOutput()
			b, _ := os.ReadFile(log)
			s := string(b)
			if mode == "unsigned" {
				if err == nil || !strings.Contains(string(out), "MYCE_DEVELOPER_ID is required") || s != "" {
					t.Fatal("missing identity did not stop before building and manifest creation", err, string(out), s)
				}
				return
			}
			s = strings.ReplaceAll(s, "build\n", "")
			if mode == "sign-fails" {
				exit, ok := err.(*exec.ExitError)
				if !ok || exit.ExitCode() != 42 || !strings.HasPrefix(s, "codesign ") || strings.Contains(s, "manifest") {
					t.Fatal("signing failure did not stop manifest creation", err, s)
				}
				return
			}
			if mode == "verify-fails" {
				exit, ok := err.(*exec.ExitError)
				if !ok || exit.ExitCode() != 43 || strings.Contains(s, "manifest") {
					t.Fatal("signature verification failure did not stop manifest creation", err, s)
				}
				return
			}
			if err != nil {
				t.Fatalf("release: %v %s", err, out)
			}
			lines := strings.Split(strings.TrimSpace(s), "\n")
			if len(lines) != 3 || !strings.HasPrefix(lines[0], "codesign ") || !strings.Contains(lines[0], "--options runtime") || !strings.Contains(lines[0], "--sign fake-test-identity") || !strings.HasSuffix(lines[0], "myce-darwin-arm64") || !strings.Contains(lines[1], "--verify --strict") || !strings.Contains(lines[1], "certificate leaf[field.1.2.840.113635.100.6.1.13] exists") || lines[2] != "manifest" {
				t.Fatal("Darwin hardened signature must be verified before manifest hashes", s)
			}
		})
	}
}
