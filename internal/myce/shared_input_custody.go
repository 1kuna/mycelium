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
type sharedInputOwnerKey struct{}
type sharedInputUse struct {
	Root string `json:"root"`
	Key  string `json:"key"`
}

type sharedInputReceipt struct {
	Owner string           `json:"owner"`
	Uses  []sharedInputUse `json:"uses"`
}

func (s *workerStore) sharedInputOwnerID() string {
	name := s.stateName
	if name == "" {
		name = "state.json" // Legacy single-attempt spool.
	}
	return identityHash([2]string{s.dir, name})
}

func (s *workerStore) sharedInputCustodyFile() string {
	return "shared-input-custody-" + s.sharedInputOwnerID() + ".json"
}

func sharedInputPin(root, key, owner string) string {
	return filepath.Join(root, "pins", key, identityHash(owner))
}

func (s *workerStore) readSharedInputUses() ([]sharedInputUse, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, s.sharedInputCustodyFile()))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	var receipt sharedInputReceipt
	if err == nil {
		err = json.Unmarshal(b, &receipt)
	}
	if err == nil && receipt.Owner != s.sharedInputOwnerID() {
		err = errors.New("mismatched shared input custody owner")
	}
	return receipt.Uses, err
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
		if err = saveWorkerJSON(s.dir, s.sharedInputCustodyFile(), sharedInputReceipt{Owner: s.sharedInputOwnerID(), Uses: append(uses, use)}); err != nil {
			return nil, err
		}
	}
	pin := sharedInputPin(root, key, s.sharedInputOwnerID())
	if err = os.MkdirAll(filepath.Dir(pin), 0700); err == nil {
		// An empty pin has no partial representation: writing the final name
		// directly avoids orphaned atomic-write temporary files after a crash.
		var f *os.File
		f, err = os.OpenFile(pin, os.O_CREATE|os.O_WRONLY, 0600)
		if err == nil {
			err = errors.Join(f.Sync(), f.Close(), syncWorkerDir(filepath.Dir(pin)))
		}
	}
	return func() { _ = removeSharedInputPin(pin) }, err
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
		pin := sharedInputPin(use.Root, use.Key, s.sharedInputOwnerID())
		if err := removeSharedInputPin(pin); err != nil {
			return err
		}
	}
	return nil
}

func removeSharedInputPin(pin string) error {
	for _, path := range []string{pin, pin + ".tmp"} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func sharedInputOwner(ctx context.Context) *workerStore {
	s, _ := ctx.Value(sharedInputOwnerKey{}).(*workerStore)
	return s
}
