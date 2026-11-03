package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// The attempt receipt is synced before the cache pin, and both precede launch.
// A crashed worker loses its flock, but only exact process/container cleanup
// removes these pins. Missing or unreadable custody never authorizes eviction.
const sharedInputCustodyFile = "shared-input-custody.json"

type sharedInputOwnerKey struct{}
type sharedInputUse struct {
	Root string `json:"root"`
	Key  string `json:"key"`
}

func sharedInputPin(root, key, owner string) string {
	return filepath.Join(root, "pins", key, identityHash(owner))
}

func (s *workerStore) readSharedInputUses() ([]sharedInputUse, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, sharedInputCustodyFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	var uses []sharedInputUse
	if err == nil {
		err = json.Unmarshal(b, &uses)
	}
	return uses, err
}

func (s *workerStore) pinSharedInput(root, key string) (func(), error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	uses, err := s.readSharedInputUses()
	if err != nil {
		return nil, err
	}
	use := sharedInputUse{Root: root, Key: key}
	found := false
	for _, previous := range uses {
		found = found || previous == use
	}
	if !found {
		if err = saveWorkerJSON(s.dir, sharedInputCustodyFile, append(uses, use)); err != nil {
			return nil, err
		}
	}
	pin := sharedInputPin(root, key, s.dir)
	if err = os.MkdirAll(filepath.Dir(pin), 0700); err == nil {
		err = saveWorkerJSON(filepath.Dir(pin), filepath.Base(pin), struct{}{})
	}
	return func() { _ = os.Remove(pin) }, err
}

func sharedInputPinned(root, key string) (bool, error) {
	entries, err := os.ReadDir(filepath.Join(root, "pins", key))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return len(entries) != 0, err
}

func (s *workerStore) releaseSharedInputPins() error {
	uses, err := s.readSharedInputUses()
	if err != nil {
		return err
	}
	for _, use := range uses {
		pin := sharedInputPin(use.Root, use.Key, s.dir)
		if err := os.Remove(pin); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func sharedInputOwner(ctx context.Context) *workerStore {
	s, _ := ctx.Value(sharedInputOwnerKey{}).(*workerStore)
	return s
}
