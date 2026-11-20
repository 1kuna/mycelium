package myce

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
)

// Keep launch metadata beside, never inside, the writable payload workdir.
// Both ordinary job-files retention and ephemeral attempt cleanup own it.
func nativeLaunchRoot(work string) string {
	return filepath.Join(filepath.Dir(work), "."+filepath.Base(work)+"-launch")
}

func writeNativeLaunchFile(work string, data []byte) (path, digest string, err error) {
	root := nativeLaunchRoot(work)
	if err = os.MkdirAll(root, 0700); err != nil {
		return "", "", err
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() {
		return "", "", errors.New("native launch metadata requires a real directory")
	}
	f, err := os.CreateTemp(root, "launch-*")
	if err != nil {
		return "", "", err
	}
	_, err = f.Write(data)
	err = errors.Join(err, f.Close())
	if err != nil {
		_ = os.Remove(f.Name())
		return "", "", err
	}
	sum := sha256.Sum256(data)
	return f.Name(), hex.EncodeToString(sum[:]), nil
}

func readNativeLaunchFile(path, digest string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("native launch metadata unavailable")
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != digest {
		return nil, errors.New("native launch metadata integrity mismatch")
	}
	return data, nil
}
