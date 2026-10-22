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
	for _, mode := range []string{"configured", "unsigned", "sign-fails"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			log := filepath.Join(dir, "calls")
			tools := map[string]string{
				"uname": `case "$1" in -s) echo Darwin;; -m) echo arm64;; esac`,
				"go": `while test "$#" -gt 0; do if test "$1" = -o; then shift; out=$1; fi; shift; done
cat > "$out" <<'BIN'
#!/bin/sh
case "$1:$2" in
release:build-targets) printf 'darwin arm64 1\nlinux amd64 0\nlinux arm64 0\n';;
release:manifest) echo manifest >> "$TEST_SIGN_LOG";;
esac
BIN
chmod +x "$out"`,
				"codesign": `echo "codesign $*" >> "$TEST_SIGN_LOG"
test "$TEST_SIGN_MODE" != sign-fails || exit 42`,
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
			if mode == "sign-fails" {
				if err == nil || strings.Contains(s, "manifest") {
					t.Fatal("signing failure did not stop manifest creation", err, s)
				}
				return
			}
			if err != nil {
				t.Fatalf("release: %v %s", err, out)
			}
			if mode == "unsigned" {
				if s != "manifest\n" {
					t.Fatal("unconfigured release used signing tools", s)
				}
				return
			}
			lines := strings.Split(strings.TrimSpace(s), "\n")
			if len(lines) != 3 || !strings.HasPrefix(lines[0], "codesign ") || !strings.Contains(lines[0], "--options runtime") || !strings.Contains(lines[0], "--sign fake-test-identity") || !strings.HasSuffix(lines[0], "myce-darwin-arm64") || !strings.Contains(lines[1], "--verify --strict") || lines[2] != "manifest" {
				t.Fatal("Darwin hardened signature must be verified before manifest hashes", s)
			}
		})
	}
}
