package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"mycelium/internal/myce"
)

// --notify is an executable, never shell text. Its configuration stays private
// to the caller. --check must fail closed if its receiver is not configured.
func profileNotifier(ctx context.Context, path string) (string, error) {
	if path == "" {
		return "", nil
	}
	resolved, err := exec.LookPath(path)
	if err != nil {
		return "", err
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return "", err
	}
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(checkCtx, resolved, "--check")
	cmd.Env = notifierEnvironment()
	if err = cmd.Run(); err != nil {
		return "", err
	}
	return resolved, nil
}
func notifierEnvironment() []string {
	var env []string
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "MYCE_TOKEN=") {
			env = append(env, v)
		}
	}
	return env
}

func armProfileObserver(ctx context.Context, dir string, r *profileReceipt, cfg profileObserver, token string) error {
	lock, err := profileLock(filepath.Join(dir, "watch.lock"))
	if err != nil {
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return err
		}
		// An OS lock, not a PID file, proves there is exactly one live observer.
		return readProfileJSON(filepath.Join(dir, "receipt.json"), r)
	}
	defer lock.Close()
	if err = readProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
		return err
	}
	if r.DeliveryStatus == "delivered" {
		return nil
	}
	var saved profileObserver
	if e := readProfileJSON(filepath.Join(dir, "observer.json"), &saved); e == nil && cfg.Notify == "" {
		cfg.Notify = saved.Notify
	}
	if cfg.Notify == "" && (r.Status == "succeeded" || r.Status == "failed" || r.Status == "cancelled") {
		return nil
	}
	cfg.Notify, err = profileNotifier(ctx, cfg.Notify)
	if err != nil {
		r.Observation, r.Delivery = "not_armed", "not_configured"
		if e := saveProfileJSON(filepath.Join(dir, "receipt.json"), r); e != nil {
			return e
		}
		return err
	}
	if err = saveProfileJSON(filepath.Join(dir, "observer.json"), cfg); err != nil {
		return err
	}
	r.Observation, r.ObservationError = "armed", ""
	r.Delivery = "not_configured"
	if cfg.Notify != "" {
		r.Delivery = "armed"
	}
	if err = saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	ready, writeReady, err := os.Pipe()
	if err != nil {
		return err
	}
	defer ready.Close()
	defer writeReady.Close()
	cmd := exec.Command(executable, "wait", "--receipt", dir)
	cmd.Env = append(notifierEnvironment(), "MYCE_TOKEN="+token, "MYCE_PROFILE_OBSERVER=1")
	cmd.ExtraFiles = []*os.File{lock, writeReady}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	// No inherited stdio pipe or terminal may keep the submitting session alive.
	if err = cmd.Start(); err != nil {
		r.Observation = "not_armed"
		_ = saveProfileJSON(filepath.Join(dir, "receipt.json"), r)
		return err
	}
	writeReady.Close()
	ack := make(chan error, 1)
	go func() { var b [1]byte; _, e := io.ReadFull(ready, b[:]); ack <- e }()
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	select {
	case err = <-ack:
	case <-startup.Done():
		err = startup.Err()
	}
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		r.Observation = "not_armed"
		_ = saveProfileJSON(filepath.Join(dir, "receipt.json"), r)
		return err
	}
	_ = cmd.Process.Release()
	return readProfileJSON(filepath.Join(dir, "receipt.json"), r)
}

type profileNotification struct {
	DeliveryID      string            `json:"delivery_id"`
	JobID           string            `json:"job_id"`
	Key             string            `json:"key"`
	Status          string            `json:"status"`
	ResultReference myce.FileManifest `json:"result_reference"`
}

// This is the detached myce wait process. The inherited flock survives caller
// exit and prevents another invocation from spawning a duplicate watcher.
func waitProfileReceipt(ctx context.Context, dir string, timeout time.Duration) error {
	if os.Getenv("MYCE_PROFILE_OBSERVER") != "1" {
		return waitError(2, "receipt observer is started by myce run --profile; rerun that command to recover")
	}
	lock := os.NewFile(3, "watch.lock")
	defer lock.Close()
	ready := os.NewFile(4, "ready")
	var r profileReceipt
	var cfg profileObserver
	if err := readProfileJSON(filepath.Join(dir, "receipt.json"), &r); err != nil {
		ready.Close()
		return err
	}
	if err := readProfileJSON(filepath.Join(dir, "observer.json"), &cfg); err != nil {
		ready.Close()
		return err
	}
	c, err := runClient(cfg.Config, cfg.Joining, cfg.Queue, os.Getenv("MYCE_TOKEN"))
	if err != nil {
		ready.Close()
		return err
	}
	if c.HTTP != nil {
		defer c.HTTP.CloseIdleConnections()
	}
	_, err = ready.Write([]byte{1})
	ready.Close()
	if err != nil {
		return err
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return observeProfile(ctx, c, dir, r, cfg)
}

func observeProfile(ctx context.Context, c myce.Client, dir string, r profileReceipt, cfg profileObserver) error {
	j, err := c.Wait(ctx, r.JobID)
	r.Observation = "not_armed"
	if err != nil {
		r.ObservationError = "transport_error"
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			r.ObservationError = "timeout"
		} else if errors.Is(ctx.Err(), context.Canceled) {
			r.ObservationError = "interrupted"
		}
		_ = saveProfileJSON(filepath.Join(dir, "receipt.json"), r)
		return waitError(2, "receipt observation ended; job continues; rerun the profile to recover")
	}
	r.Status, r.ResultReference = j.State, j.OutputRefs
	if err = saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
		return err
	}
	if cfg.Notify == "" {
		return nil
	}
	notification := profileNotification{DeliveryID: profileHash([]string{r.Project, r.Key, r.JobID}), JobID: r.JobID, Key: r.Key, Status: r.Status, ResultReference: r.ResultReference}
	b, _ := json.Marshal(notification)
	// Notifier owns its HTTP retry/deadline policy. No logs or queue reason text
	// are passed; a crash after delivery may replay this same delivery_id.
	cmd := exec.CommandContext(ctx, cfg.Notify)
	cmd.Stdin = strings.NewReader(string(b))
	cmd.Env = notifierEnvironment()
	err = cmd.Run()
	r.DeliveryStatus = "delivered"
	if err != nil {
		r.DeliveryStatus = "failed"
	}
	if e := saveProfileJSON(filepath.Join(dir, "receipt.json"), r); e != nil {
		return e
	}
	return err
}
