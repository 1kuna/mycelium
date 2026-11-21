package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOrphanCompilerPinRequiresExactFinishedCustody(t *testing.T) {
	_, clock := fixture(t)
	for _, tc := range []struct {
		name      string
		edit      func(*custodianSnapshot)
		birth     func(int) (string, error)
		manager   error
		active    bool
		wantFreed bool
	}{
		{name: "independent finished proof", wantFreed: true},
		{name: "still running", edit: func(s *custodianSnapshot) { s.Finished = false }},
		{name: "release pending", edit: func(s *custodianSnapshot) { s.State.Result.ReleasePending = true }},
		{name: "preparation retained", edit: func(s *custodianSnapshot) { s.State.Preparation = "builder" }},
		{name: "wrong execution", edit: func(s *custodianSnapshot) { s.State.Result.Attempt++ }},
		{name: "exact process still lives", birth: func(int) (string, error) { return "helper-birth", nil }},
		{name: "inspection refusal", birth: func(int) (string, error) { return "", os.ErrPermission }},
		{name: "manager still owns helper", manager: errors.New("scope active")},
		{name: "coordinator still owns attempt", active: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := &workerStore{dir: filepath.Join(t.TempDir(), "state")}
			if err := os.Mkdir(root.dir, 0700); err != nil {
				t.Fatal(err)
			}
			dir := custodianDir(root.dir)
			if err := PrepareStateRoots(dir); err != nil {
				t.Fatal(err)
			}
			cache := &workerCompilerCache{cache: EnvironmentCache{Root: filepath.Join(root.dir, "compiler-cache"), Clock: clock}}
			if err := cache.cache.initStorage(); err != nil {
				t.Fatal(err)
			}
			id := ID()
			job := &Job{ID: ID(), Attempt: 1, Session: "session", Machine: "machine"}
			state := workerState{Identity: ID(), Session: job.Session, Job: job}
			req := custodianRequest{Protocol: custodianProtocol, ID: id, Machine: Machine{ID: job.Machine}, State: state}
			owner := &workerStore{dir: root.dir, stateName: custodianName(id, "state")}
			key := identityHash("go-build")
			if _, err := owner.pinSharedInput(cache.cache.Root, key); err != nil {
				t.Fatal(err)
			}
			state.Result = &Result{Execution: Execution{ID: job.ID, Attempt: job.Attempt}, Session: job.Session, Machine: job.Machine}
			snap := custodianSnapshot{Protocol: custodianProtocol, ID: id, PID: 4242, Birth: "helper-birth", At: clock.Now(), Finished: true, State: state}
			if tc.edit != nil {
				tc.edit(&snap)
			}
			if err := saveWorkerJSON(dir, custodianName(id, "request"), req); err != nil {
				t.Fatal(err)
			}
			if err := saveWorkerJSON(dir, custodianName(id, "snapshot"), snap); err != nil {
				t.Fatal(err)
			}
			birth := tc.birth
			if birth == nil {
				birth = func(int) (string, error) { return "", os.ErrNotExist }
			}
			w := Worker{Machine: req.Machine, Clock: clock, compilerCache: cache, custodianBirth: birth,
				custodianAbsent: func(context.Context, string) error { return tc.manager }}
			var attempts []*workerStore
			if tc.active {
				attempts = []*workerStore{{state: req.State}}
			}
			w.reconcileFinishedCompilerPins(context.Background(), root, attempts)
			if pinned, err := sharedInputPinned(cache.cache.Root, key); err != nil || pinned == tc.wantFreed {
				t.Fatalf("compiler pin = %t (%v), want %t", pinned, err, !tc.wantFreed)
			}
		})
	}
}
