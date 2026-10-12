package myce

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
)

// One atomic tombstone preserves custody and makes all existing receipts agree.
type forgottenQueues struct {
	Generation uint64            `json:"generation"`
	Queues     map[string]uint64 `json:"queues"`
}

func readForgottenQueues(dir string) (forgottenQueues, error) {
	st := forgottenQueues{Queues: map[string]uint64{}}
	b, err := os.ReadFile(filepath.Join(dir, "forgotten-queues.json"))
	if errors.Is(err, os.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	err = json.Unmarshal(b, &st)
	return st, err
}
func (h HostControl) Queues() ([]string, error) {
	workers, err := h.workers()
	if err != nil {
		return nil, err
	}
	var queues []string
	for _, w := range workers {
		if w.Queue != "" && !slices.Contains(queues, w.Queue) {
			queues = append(queues, w.Queue)
		}
	}
	slices.Sort(queues)
	return queues, nil
}
func (h HostControl) ForgetQueue(queue string, force bool, configured func(HostWorker) (bool, error)) error {
	if queue == "" {
		return errors.New("queue URL required")
	}
	if err := h.defaults(); err != nil {
		return err
	}
	unlock, err := hostOperationLock(h.Dir, "control.lock", false)
	if err != nil {
		return err
	}
	defer unlock()
	// Registration uses the same locks, including while the host is disabled.
	launchUnlock, err := hostOperationLock(h.Dir, "launch.lock", true)
	if err != nil {
		return err
	}
	defer launchUnlock()
	workers, err := h.workers()
	if err != nil {
		return err
	}
	found := false
	for _, w := range workers {
		if w.Queue != queue {
			continue
		}
		found = true
		if !force {
			if configured == nil {
				return errors.New("configured-worker check required")
			}
			used, e := configured(w)
			if e != nil {
				return e
			}
			if used {
				return errors.New("queue used by a configured local worker; use --force to forget")
			}
		}
	}
	if !found {
		return errors.New("queue registration not found")
	}
	st, err := readForgottenQueues(h.Dir)
	if err != nil {
		return err
	}
	generation, err := nextHostGeneration(h.Dir)
	if err != nil {
		return err
	}
	if st.Queues == nil {
		st.Queues = map[string]uint64{}
	}
	st.Generation = generation
	st.Queues[queue] = generation
	return saveWorkerJSON(h.Dir, "forgotten-queues.json", st)
}
