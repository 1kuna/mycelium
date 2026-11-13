package myce

import (
	"errors"
	"fmt"
	"strings"
)

// MachineKind names a kind of machine for production approval (MY-51): OS,
// CPU architecture and accelerator platform, never a specific host. An empty
// field matches any value, so "cuda" approves CUDA on every OS and
// architecture, and "darwin/arm64/metal" approves only Apple Silicon Metal.
// The work identity selects the software variant that runs on that kind.
type MachineKind struct {
	OS       string
	Arch     string
	Platform string
}

var (
	kindOSAliases   = map[string]string{"linux": "linux", "darwin": "darwin", "mac": "darwin", "macos": "darwin", "osx": "darwin", "windows": "windows", "win": "windows"}
	kindArchAliases = map[string]string{"amd64": "amd64", "x86_64": "amd64", "x86": "amd64", "x64": "amd64", "arm64": "arm64", "aarch64": "arm64", "arm": "arm64"}
	// Apple Silicon workers report their accelerator as metal; MLX and MPS
	// software runs on it.
	kindPlatformAliases = map[string]string{"mlx": "metal", "mps": "metal"}
)

func kindToken(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
			return false
		}
	}
	return true
}

func kindPlatform(s string) string {
	if alias, ok := kindPlatformAliases[s]; ok {
		return alias
	}
	return s
}

// ParseMachineKind reads either os/arch/platform, where * or an empty part
// matches anything, or a comma-separated list of words such as "cuda",
// "mac", "linux,x86" or "arm,metal". Each word is an OS, an architecture or
// an accelerator platform.
func ParseMachineKind(s string) (MachineKind, error) {
	text := strings.ToLower(strings.TrimSpace(s))
	var k MachineKind
	invalid := func(why string) (MachineKind, error) {
		return MachineKind{}, fmt.Errorf("machine kind %q: %s; name an OS (linux, mac, windows), an architecture (x86, arm) and/or an accelerator platform (cuda, xpu, metal, cpu), for example cuda, mac or linux/amd64/cuda", s, why)
	}
	if strings.Contains(text, "/") {
		parts := strings.Split(text, "/")
		if len(parts) != 3 {
			return invalid("the slash form is os/arch/platform")
		}
		for i, p := range parts {
			p = strings.TrimSpace(p)
			if p == "*" || p == "" {
				continue
			}
			if !kindToken(p) {
				return invalid("unexpected characters")
			}
			switch i {
			case 0:
				if alias, ok := kindOSAliases[p]; ok {
					p = alias
				}
				k.OS = p
			case 1:
				if alias, ok := kindArchAliases[p]; ok {
					p = alias
				}
				k.Arch = p
			default:
				k.Platform = kindPlatform(p)
			}
		}
	} else {
		for word := range strings.SplitSeq(text, ",") {
			word = strings.TrimSpace(word)
			if !kindToken(word) {
				return invalid("each word must be nonempty letters, digits, '-', '_' or '.'")
			}
			field, value := &k.Platform, kindPlatform(word)
			if os, ok := kindOSAliases[word]; ok {
				field, value = &k.OS, os
			} else if arch, ok := kindArchAliases[word]; ok {
				field, value = &k.Arch, arch
			}
			if *field != "" && *field != value {
				return invalid("two values for one of OS, architecture or platform")
			}
			*field = value
		}
	}
	if k == (MachineKind{}) {
		return invalid("it matches every machine")
	}
	return k, nil
}

func (k MachineKind) String() string {
	part := func(s string) string {
		if s == "" {
			return "*"
		}
		return s
	}
	return part(k.OS) + "/" + part(k.Arch) + "/" + part(k.Platform)
}

func (k MachineKind) MarshalText() ([]byte, error) { return []byte(k.String()), nil }

func (k *MachineKind) UnmarshalText(b []byte) error {
	if len(b) == 0 {
		return errors.New("machine kind required")
	}
	parsed, err := ParseMachineKind(string(b))
	if err != nil {
		return err
	}
	*k = parsed
	return nil
}

// Matches reports whether a job running as target t runs on this kind.
func (k MachineKind) Matches(t ExecutionTarget) bool {
	return (k.OS == "" || k.OS == t.OS) && (k.Arch == "" || k.Arch == t.Arch) && (k.Platform == "" || k.Platform == t.Platform)
}

// exactKind is the full kind of one target; it is empty when the host has not
// reported its OS and architecture.
func exactKind(t ExecutionTarget) (MachineKind, bool) {
	if t.OS == "" || t.Arch == "" || t.Platform == "" || t.OS == "unknown" || t.Arch == "unknown" {
		return MachineKind{}, false
	}
	return MachineKind{OS: t.OS, Arch: t.Arch, Platform: t.Platform}, true
}
