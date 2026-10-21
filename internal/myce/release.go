package myce

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"golang.org/x/mod/semver"
)

const StartGateProtocol = startGateProtocol
const AttemptProtocol = keyedAttemptProtocol

var BuildVersion = "development"

type ReleaseArtifact struct {
	OS            string `json:"os"`
	Arch          string `json:"arch"`
	File          string `json:"file"`
	SHA256        string `json:"sha256"`
	Size          int64  `json:"size"`
	NativeSensing bool   `json:"native_sensing"`
}
type ReleaseManifest struct {
	Version         string            `json:"version"`
	StartGate       string            `json:"start_gate"`
	AttemptProtocol string            `json:"attempt_protocol"`
	Artifacts       []ReleaseArtifact `json:"artifacts"`
}
type SignedRelease struct {
	Manifest  json.RawMessage `json:"manifest"`
	Signature []byte          `json:"signature"`
}
type VersionInfo struct {
	Version         string `json:"version"`
	OS              string `json:"os"`
	Arch            string `json:"arch"`
	StartGate       string `json:"start_gate"`
	AttemptProtocol string `json:"attempt_protocol"`
	NativeSensing   bool   `json:"native_sensing"`
}

func Version() VersionInfo {
	return VersionInfo{BuildVersion, runtime.GOOS, runtime.GOARCH, startGateProtocol, keyedAttemptProtocol, NativeOwnerSignals}
}

var releaseName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,79}$`)

type ReleaseTarget struct {
	OS            string `json:"os"`
	Arch          string `json:"arch"`
	NativeSensing bool   `json:"native_sensing"`
}

// ReleaseTargets is shared by the publisher, validator and release build script.
var ReleaseTargets = []ReleaseTarget{{"linux", "amd64", false}, {"linux", "arm64", false}, {"darwin", "arm64", true}}

func CheckReleaseUpgrade(target, installed string, allowDowngrade bool) error {
	if !semver.IsValid(target) || !semver.IsValid(installed) {
		return errors.New("release_version_unordered: version comparison requires semver releases")
	}
	if semver.Compare(target, installed) < 0 && !allowDowngrade {
		return errors.New("release_downgrade_refused: use --allow-downgrade explicitly to install an older release")
	}
	return nil
}

func (m ReleaseManifest) Validate() error {
	if !semver.IsValid(m.Version) || !releaseName.MatchString(m.Version) || m.StartGate != startGateProtocol || m.AttemptProtocol != keyedAttemptProtocol {
		return errors.New("release version or worker protocol incompatible")
	}
	required := map[string]ReleaseTarget{}
	for _, target := range ReleaseTargets {
		required[target.OS+"/"+target.Arch] = target
	}
	seen := map[string]bool{}
	for _, a := range m.Artifacts {
		key := a.OS + "/" + a.Arch
		target, supported := required[key]
		if seen[key] || !supported || a.File != "myce-"+a.OS+"-"+a.Arch || !validPin(a.SHA256) || a.Size <= 0 || target.NativeSensing && !a.NativeSensing {
			return errors.New("invalid release matrix, checksum or native Mac sensing")
		}
		seen[key] = true
	}
	if len(seen) != len(required) {
		return errors.New("release requires every configured target")
	}
	return nil
}
func (m ReleaseManifest) Select(goos, arch string) (ReleaseArtifact, error) {
	if err := m.Validate(); err != nil {
		return ReleaseArtifact{}, err
	}
	for _, a := range m.Artifacts {
		if a.OS == goos && a.Arch == arch {
			return a, nil
		}
	}
	return ReleaseArtifact{}, errors.New("unsupported release OS/architecture")
}
func VerifyRelease(data []byte, key ed25519.PublicKey) (ReleaseManifest, error) {
	var signed SignedRelease
	var m ReleaseManifest
	if len(key) != ed25519.PublicKeySize || json.Unmarshal(data, &signed) != nil || !ed25519.Verify(key, signed.Manifest, signed.Signature) {
		return m, errors.New("release manifest signature refused")
	}
	if err := json.Unmarshal(signed.Manifest, &m); err != nil {
		return m, errors.New("invalid release manifest")
	}
	return m, m.Validate()
}
func ReadReleasePublicKey(path string) (ed25519.PublicKey, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	p, _ := pem.Decode(b)
	if p == nil {
		return nil, errors.New("release key requires a provisioned Ed25519 public PEM")
	}
	v, err := x509.ParsePKIXPublicKey(p.Bytes)
	if err != nil {
		return nil, err
	}
	k, ok := v.(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("release key must be Ed25519")
	}
	return k, nil
}
func SignRelease(m ReleaseManifest, keyPath string) ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	st, err := os.Lstat(keyPath)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		return nil, errors.New("release signing key must be a private regular file")
	}
	b, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}
	p, _ := pem.Decode(b)
	if p == nil {
		return nil, errors.New("release signing key must be PKCS8 Ed25519 PEM")
	}
	v, err := x509.ParsePKCS8PrivateKey(p.Bytes)
	if err != nil {
		return nil, errors.New("invalid release signing key")
	}
	k, ok := v.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("release signing key must be Ed25519")
	}
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return json.Marshal(SignedRelease{data, ed25519.Sign(k, data)})
}
func FileSHA256(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(b)
	return hex.EncodeToString(hash[:]), nil
}
func releaseOrigin(origin string) error {
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.ContainsAny(origin, "\r\n") {
		return errors.New("release requires a trusted publisher HTTPS URL")
	}
	return nil
}
func quoteShell(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }

// The authenticated inviter embeds only public release metadata and a limited
// ticket. No downloaded shell script runs ahead of checksum verification.
func BootstrapCommand(m ReleaseManifest, origin, queue, pin, ticket string) (string, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}
	if err := releaseOrigin(origin); err != nil {
		return "", err
	}
	if err := joiningURL(queue); err != nil {
		return "", err
	}
	if !validPin(pin) || !validPin(ticket) {
		return "", errors.New("invalid invite pin or ticket")
	}
	var matrix strings.Builder
	for _, a := range m.Artifacts {
		fmt.Fprintf(&matrix, "%s/%s) file=%s; hash=%s; size=%d;;\n", a.OS, a.Arch, quoteShell(a.File), quoteShell(a.SHA256), a.Size)
	}
	script := `set -eu
umask 077
os=$(uname -s); arch=$(uname -m)
case "$os" in Linux) os=linux;; Darwin) os=darwin;; *) echo "unsupported operating system" >&2; exit 1;; esac
case "$arch" in x86_64|amd64) arch=amd64;; aarch64|arm64) arch=arm64;; *) echo "unsupported architecture" >&2; exit 1;; esac
case "$os/$arch" in
` + matrix.String() + `*) echo "unsupported release target" >&2; exit 1;; esac
cache="$HOME/.cache/mycelium/bootstrap"
mkdir -p "$cache"
test ! -L "$cache" || exit 1
chmod 700 "$cache"
part="$cache/$hash.partial"; binary="$cache/$hash"
test ! -L "$part" && test ! -L "$binary" || exit 1
checksum() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1"; else shasum -a 256 "$1"; fi; }
verify() { test "$(wc -c < "$1" | tr -d " ")" = "$size" && test "$(checksum "$1" | cut -d " " -f 1)" = "$hash"; }
if ! test -f "$binary" || ! verify "$binary"; then
  if test -f "$part" && ! verify "$part" && test "$(wc -c < "$part" | tr -d " ")" -ge "$size"; then
    rm -f "$part"; echo "release_partial_invalid: discarded invalid full-size partial; rerun to download" >&2; exit 1
  fi
  if ! test -f "$part" || ! verify "$part"; then
    code=$(curl --fail --silent --show-error --proto =https --tlsv1.2 --connect-timeout 15 --max-time 600 --retry 3 --continue-at - --output "$part" --write-out "%{http_code}" ` + quoteShell(strings.TrimRight(origin, "/")+"/") + `"$file") || { echo "download incomplete; rerun to resume" >&2; exit 1; }
    case "$code" in 200|206) ;; *) rm -f "$part"; echo "release_redirect_refused: discarded response body" >&2; exit 1;; esac
  fi
  verify "$part" || { rm -f "$part"; echo "release_checksum_refused: release checksum refused; discarded partial" >&2; exit 1; }
  mv "$part" "$binary"
fi
chmod 700 "$binary"
"$binary" version --expect-version ` + quoteShell(m.Version) + ` --expect-os "$os" --expect-arch "$arch" --expect-start-gate ` + quoteShell(m.StartGate) + ` --expect-attempt-protocol ` + quoteShell(m.AttemptProtocol) + ` --require-native-sensing
exec "$binary" install worker --enroll --queue ` + quoteShell(queue) + ` --pin ` + quoteShell(pin) + ` --ticket-stdin --release-version ` + quoteShell(m.Version) + ` --release-sha "$hash" "$@" <<'MYCE_ENROLLMENT_TICKET'
` + ticket + `
MYCE_ENROLLMENT_TICKET
`
	// A single physical command; base64 is available on both supported OSes.
	return "printf %s " + quoteShell(base64.StdEncoding.EncodeToString([]byte(script))) + " | base64 -d | sh -s --", nil
}

func ReleaseArtifactPath(dir string, a ReleaseArtifact) string { return filepath.Join(dir, a.File) }
