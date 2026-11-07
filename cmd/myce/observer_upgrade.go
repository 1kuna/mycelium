package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/mod/semver"
	"mycelium/internal/myce"
)

type observerUpgradeRequest struct {
	Version        string    `json:"version"`
	Executable     string    `json:"executable"`
	SHA256         string    `json:"sha256"`
	OwnerPID       int       `json:"owner_pid"`
	OwnerStartedAt time.Time `json:"owner_started_at"`
}

func newerObserverVersion(candidate, current string) bool {
	return semver.IsValid(candidate) && semver.IsValid(current) && semver.Compare(candidate, current) > 0
}

// Called under startup.lock. A cooperative owner, rather than a submitter,
// transfers its lifetime lock at an idle point with every registration durable.
// Legacy owners without status metadata require explicit operator recovery.
func requestObserverUpgrade(root string) error {
	var state observerDiagnostic
	if err := readProfileJSON(filepath.Join(root, "observer.status"), &state); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !newerObserverVersion(myce.BuildVersion, state.Version) {
		return nil
	}
	path, err := os.Executable()
	if err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(data)
	request := observerUpgradeRequest{Version: myce.BuildVersion, Executable: path, SHA256: hex.EncodeToString(hash[:]), OwnerPID: state.PID, OwnerStartedAt: state.StartedAt}
	var existing observerUpgradeRequest
	if readProfileJSON(filepath.Join(root, "observer.upgrade"), &existing) == nil && existing.OwnerPID == state.PID && newerObserverVersion(existing.Version, request.Version) {
		return nil
	}
	if err := saveProfileJSON(filepath.Join(root, "observer.upgrade"), request); err != nil {
		return err
	}
	// Publish before waking: an already-adopted receipt may not create a new
	// registration, and discovery can otherwise be sleeping adaptively.
	if socketPath, err := sharedProfileSocket(root); err == nil {
		if wake, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: socketPath, Net: "unixgram"}); err == nil {
			_, _ = wake.Write([]byte{1})
			wake.Close()
		}
	}
	return nil
}

func requestAdoptedObserverUpgrade(ctx context.Context) error {
	root, err := sharedProfileRoot()
	if err != nil {
		return err
	}
	var state observerDiagnostic
	if readProfileJSON(filepath.Join(root, "observer.status"), &state) != nil || !newerObserverVersion(myce.BuildVersion, state.Version) {
		return nil
	}
	bounded, cancel := context.WithCancel(ctx)
	defer cancel()
	start, err := sharedProfileLock(bounded, filepath.Join(root, "startup.lock"))
	if err != nil {
		return err
	}
	defer start.Close()
	return requestObserverUpgrade(root)
}

func tryObserverHandoff(ctx context.Context, root string, lock *os.File, watches map[string]*sharedProfileWatch, diagnostics *observerDiagnostics, quiesce ...func()) (bool, error) {
	path := filepath.Join(root, "observer.upgrade")
	var request observerUpgradeRequest
	if err := readProfileJSON(path, &request); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	start, err := profileLock(filepath.Join(root, "startup.lock"))
	if err != nil {
		return false, nil
	}
	defer start.Close()
	if err = readProfileJSON(path, &request); err != nil {
		return false, err
	}
	if request.OwnerPID != os.Getpid() || !request.OwnerStartedAt.Equal(diagnostics.state.StartedAt) || !newerObserverVersion(request.Version, diagnostics.state.Version) {
		return false, os.Remove(path)
	}
	if !filepath.IsAbs(request.Executable) {
		return false, errors.New("observer upgrade executable is not absolute")
	}
	data, err := os.ReadFile(request.Executable)
	if err != nil {
		return false, err
	}
	hash := sha256.Sum256(data)
	if hex.EncodeToString(hash[:]) != request.SHA256 {
		return false, errors.New("observer upgrade image changed")
	}
	if err = diagnostics.record("handoff", "", nil, true); err != nil {
		return false, err
	}
	// No notifier runs here. Release only receipt locks; the inherited lifetime
	// descriptor keeps the registry exclusively owned across child startup.
	for _, stop := range quiesce {
		stop()
	}
	for key, w := range watches {
		w.lock.Close()
		delete(watches, key)
	}
	if err = os.Remove(path); err != nil {
		return false, err
	}
	if err = startSharedObserver(ctx, request.Executable, root, lock); err != nil {
		// The old owner remains alive and reloads all durable arms on its next pass.
		_ = saveProfileJSON(path, request)
		return false, err
	}
	return true, nil
}
