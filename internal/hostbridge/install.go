package hostbridge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type InstallHooks struct {
	Secure       func(string) error
	Registration func() (string, error)
	Register     func(string) error
	Unregister   func() error
}

func StartupCommand(root string) string {
	return `"` + filepath.Join(root, "myce-host-bridge.exe") + `" serve`
}

// Install refuses to overwrite either existing data or another startup entry.
// Secure must set a protected per-user inherited DACL BEFORE writing secrets.
func Install(root, source string, h InstallHooks) error {
	value, err := h.Registration()
	if err != nil {
		return err
	}
	if value != "" {
		return errors.New("startup entry already exists")
	}
	if strings.ContainsAny(root, "\"\r\n") {
		return errors.New("unsupported installation path")
	}
	if err = os.Mkdir(root, 0700); err != nil {
		return errors.New("bridge directory already exists or cannot be created")
	}
	if err = h.Secure(root); err != nil {
		_ = os.Remove(root)
		return err
	}
	// Roll back only the exact files we create; never recursive-delete a folder.
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(filepath.Join(root, "myce-host-bridge.exe"))
			_ = os.Remove(filepath.Join(root, "bridge.json"))
			_ = os.Remove(root)
		}
	}()
	key := make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		return err
	}
	config := Config{Address: "127.0.0.1:47353", Key: hex.EncodeToString(key)}
	raw, err := json.Marshal(config)
	if err != nil {
		return err
	}
	if err = writeNew(filepath.Join(root, "bridge.json"), raw); err != nil {
		return err
	}
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	dest, err := os.OpenFile(filepath.Join(root, "myce-host-bridge.exe"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0700)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(dest, input)
	err = errors.Join(copyErr, dest.Close())
	if err != nil {
		return err
	}
	if err = h.Register(StartupCommand(root)); err != nil {
		// Registry writes can be uncertain: leave the installation intact if the
		// entry now refers to it, so a retry can inspect/uninstall it safely.
		v, e := h.Registration()
		if e != nil || v != "" {
			committed = true
		}
		return err
	}
	committed = true
	return nil
}
func writeNew(path string, body []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(body)
	return errors.Join(err, f.Close())
}

// Uninstall removes only owned names and a matching Run entry. Revoking config
// makes the running helper shut down within a second. Run this from the original
// downloaded binary, not the installed copy, so Windows can remove the latter.
func Uninstall(ctx context.Context, root, source string, h InstallHooks) error {
	if strings.EqualFold(filepath.Clean(source), filepath.Join(root, "myce-host-bridge.exe")) {
		return errors.New("run uninstall from the original downloaded helper")
	}
	v, err := h.Registration()
	if err != nil {
		return err
	}
	if v != "" && v != StartupCommand(root) {
		return errors.New("startup entry is not owned by this installation")
	}
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("installation root is not a regular directory")
	}
	if v != "" {
		if err = h.Unregister(); err != nil {
			return err
		}
	}
	if err = os.Remove(filepath.Join(root, "bridge.json")); err != nil && !os.IsNotExist(err) {
		return err
	}
	// A running Windows executable remains locked until serve observes revocation.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		err = os.Remove(filepath.Join(root, "myce-host-bridge.exe"))
		if err == nil || os.IsNotExist(err) {
			break
		}
		select {
		case <-ctx.Done():
			return errors.New("helper still running; startup removed and credentials revoked; retry uninstall")
		case <-tick.C:
		}
	}
	if err = os.Remove(root); err != nil {
		return fmt.Errorf("owned files removed; installation folder retained: %w", err)
	}
	return nil
}
