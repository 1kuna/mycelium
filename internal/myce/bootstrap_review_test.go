package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Execute the generated bootstrap, using only local fake downloads and a child
// that inspects its own process inputs. Never print the synthetic ticket.
func TestBootstrapTicketAndInvalidDownloadRecovery(t *testing.T) {
	for _, mode := range []string{"ticket", "full-partial", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			home := t.TempDir()
			bin := filepath.Join(home, "bin")
			if err := os.Mkdir(bin, 0700); err != nil {
				t.Fatal(err)
			}
			ticket := strings.Repeat("b", 64)
			child := `#!/bin/sh
set -eu
if test "$1" = version; then exit 0; fi
for arg in "$@"; do case "$arg" in *` + ticket + `*) exit 41;; esac; done
if env | /usr/bin/grep -q '` + ticket + `'; then exit 42; fi
read -r secret
test "$secret" = '` + ticket + `' || exit 43
echo child-inputs-private
`
			data := []byte(child)
			digest := sha256.Sum256(data)
			hash := hex.EncodeToString(digest[:])
			source := filepath.Join(home, "source")
			if err := os.WriteFile(source, data, 0700); err != nil {
				t.Fatal(err)
			}
			curl := `#!/bin/sh
set -eu
out=
while test "$#" -gt 0; do if test "$1" = --output; then shift; out=$1; fi; shift; done
`
			if mode == "redirect" {
				curl += `printf redirect > "$out"; printf 302`
			} else {
				curl += `if test -f "$out" && test "$(wc -c < "$out" | tr -d ' ')" -ge "$(wc -c < "$SOURCE" | tr -d ' ')"; then printf 416; exit 22; fi
cp "$SOURCE" "$out"; printf 200`
			}
			for name, body := range map[string]string{"curl": curl, "uname": "#!/bin/sh\ncase \"$1\" in -s) echo Linux;; -m) echo x86_64;; esac\n"} {
				if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0700); err != nil {
					t.Fatal(err)
				}
			}
			_, _, m := signedTestRelease(t)
			for i := range m.Artifacts {
				m.Artifacts[i].Size = int64(len(data))
				m.Artifacts[i].SHA256 = hash
			}
			cache := filepath.Join(home, ".cache/mycelium/bootstrap")
			if err := os.MkdirAll(cache, 0700); err != nil {
				t.Fatal(err)
			}
			part := filepath.Join(cache, hash+".partial")
			if mode == "full-partial" {
				if err := os.WriteFile(part, []byte(strings.Repeat("x", len(data))), 0600); err != nil {
					t.Fatal(err)
				}
			}
			command, err := BootstrapCommand(m, "https://publisher.invalid", "https://queue.invalid", strings.Repeat("a", 64), ticket)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cmd := exec.CommandContext(ctx, "sh", "-c", command)
			cmd.Env = []string{"HOME=" + home, "PATH=" + bin + ":/usr/bin:/bin", "SOURCE=" + source}
			out, err := cmd.CombinedOutput()
			if mode == "ticket" {
				if err != nil || !strings.Contains(string(out), "child-inputs-private") {
					t.Fatal("ticket leaked or stdin missing", err)
				}
				return
			}
			code := "release_partial_invalid"
			if mode == "redirect" {
				code = "release_redirect_refused"
			}
			if err == nil || !strings.Contains(string(out), code) {
				t.Fatal("missing typed download refusal", mode, err, string(out))
			}
			if _, err = os.Stat(part); !os.IsNotExist(err) {
				t.Fatal("invalid partial retained", err)
			}
			// One rerun must recover from a discarded corrupt full-size file.
			if mode == "full-partial" {
				cmd = exec.CommandContext(ctx, "sh", "-c", command)
				cmd.Env = []string{"HOME=" + home, "PATH=" + bin + ":/usr/bin:/bin", "SOURCE=" + source}
				if _, err = cmd.CombinedOutput(); err != nil {
					t.Fatal("fresh retry did not recover", err)
				}
			}
		})
	}
}
