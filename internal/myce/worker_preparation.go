package myce

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

type preparationCustodyKey struct{}

// The environment build lock protects the original receipt while this copy is
// saved. Cleanup thereafter uses this exact helper, never a peer's later build.
func (s *workerStore) retainPreparation(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var helper workerState
	if err := decodeWorkerJSON(data, &helper); err != nil {
		return err
	}
	name := "preparation-" + ID() + ".json"
	if err := saveWorkerJSON(s.dir, name, helper); err != nil {
		return err
	}
	s.state.Preparation = name
	return s.save()
}

func (s *workerStore) cleanupPreparation(clock Clock) error {
	name := s.state.Preparation
	if filepath.Base(name) != name || !strings.HasPrefix(name, "preparation-") || !strings.HasSuffix(name, ".json") {
		return errors.New("invalid preparation custody")
	}
	data, err := os.ReadFile(filepath.Join(s.dir, name))
	if err != nil {
		return err
	}
	var state workerState
	if err := decodeWorkerJSON(data, &state); err != nil {
		return err
	}
	// Preparation never has nested custody or a payload. Preserve this receipt
	// as proof across a restart until the attempt's release is acknowledged.
	if state.Preparation != "" || state.Job != nil {
		return errors.New("invalid helper custody")
	}
	helper := &workerStore{dir: s.dir, stateName: name, state: state}
	return helper.cleanupOrphan(clock)
}
