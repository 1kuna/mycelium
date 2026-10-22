package myce

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"
)

const darwinBundleIdentifier = "io.github.1kuna.mycelium"

// Native updates must retain the designated requirement TCC approved. In
// particular, never put an ad-hoc build between Developer ID releases. The
// existing install transaction already publishes with a same-directory atomic
// rename; validate before any publication or service operation.
func validateDarwinUpgradeIdentity(ctx context.Context, old, next string, command func(context.Context, string, ...string) ([]byte, error)) error {
	if !nativeMachO(old) && !nativeMachO(next) {
		return nil // Non-native command wrappers have no Mach-O signing identity.
	}
	refuse := errors.New("Mac update would change privacy identity; preserve the installed binary and use the same Developer ID requirement and bundle identifier; legacy ad-hoc installs require an explicit identity migration")
	requirements := []string{}
	for _, path := range []string{old, next} {
		if _, err := command(ctx, "codesign", "--verify", "--strict", path); err != nil {
			return refuse
		}
		metadata, err := command(ctx, "codesign", "-dv", "--verbose=4", path)
		if err != nil || !strings.Contains("\n"+string(metadata), "\nIdentifier="+darwinBundleIdentifier+"\n") || !strings.Contains(string(metadata), "Authority=Developer ID Application:") || strings.Contains(string(metadata), "Signature=adhoc") || !strings.Contains(string(metadata), "(runtime)") {
			return refuse
		}
		data, err := command(ctx, "codesign", "-dr", "-", path)
		if err != nil {
			return refuse
		}
		requirement := ""
		for _, line := range strings.Split(string(data), "\n") {
			if text, ok := strings.CutPrefix(line, "designated => "); ok {
				requirement = strings.TrimSpace(text)
			}
		}
		if !strings.Contains(requirement, `identifier "`+darwinBundleIdentifier+`"`) || !strings.Contains(requirement, "anchor apple") || strings.Contains(requirement, "cdhash") {
			return refuse
		}
		requirements = append(requirements, requirement)
	}
	if requirements[0] != requirements[1] {
		return refuse
	}
	return nil
}

func nativeMachO(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	var header [4]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return false
	}
	switch binary.BigEndian.Uint32(header[:]) {
	case 0xfeedface, 0xfeedfacf, 0xcefaedfe, 0xcffaedfe, 0xcafebabe, 0xbebafeca, 0xcafebabf, 0xbfbafeca:
		return true
	}
	return false
}
