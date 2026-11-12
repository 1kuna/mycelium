package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
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
	if isHarnessWake(path) {
		return path, checkHarnessWake(path, notifierEnvironment())
	}
	resolved, err := resolveProfileNotifier(path)
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

// resolveProfileNotifier shares executable lookup with read-only diagnosis;
// only profileNotifier executes the receiver's configuration check.
func resolveProfileNotifier(path string) (string, error) {
	resolved, err := exec.LookPath(path)
	if err != nil {
		return "", err
	}
	return filepath.Abs(resolved)
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

func armProfileObserver(ctx context.Context, dir string, r *profileReceipt, cfg profileObserver, token string, errOut io.Writer) error {
	// An omitted notifier inherits a saved binding exactly as before; only an
	// unbound arm takes the caller's own harness wake.
	defaultWake := cfg.Notify == harnessWakeDefault
	if defaultWake {
		cfg.Notify = ""
	}
	lock, err := profileLock(filepath.Join(dir, "watch.lock"))
	if err != nil {
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return err
		}
		// An OS lock, not a PID file, proves there is exactly one live observer.
		if err := requestAdoptedObserverUpgrade(ctx); err != nil {
			return err
		}
		fmt.Fprintln(errOut, "adopted existing watcher for receipt:", dir)
		if err := readProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
			return err
		}
		var saved profileObserver
		if err := readProfileJSON(filepath.Join(dir, "observer.json"), &saved); err != nil {
			return err
		}
		// An omitted notifier inherits the saved binding. Compare explicit hooks
		// by the same executable resolution used when arming, without running
		// --check on a hook this observer will never use.
		if cfg.Notify != "" {
			resolved, err := cfg.Notify, error(nil)
			if !isHarnessWake(resolved) {
				resolved, err = exec.LookPath(cfg.Notify)
			}
			if err == nil && !isHarnessWake(resolved) {
				resolved, err = filepath.Abs(resolved)
			}
			if err != nil || resolved != saved.Notify {
				// This is the caller's view; preserve the owner's receipt and hook.
				r.Delivery = "adopted_other_notifier"
			}
		}
		return nil
	}
	defer lock.Close()
	if err = readProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
		return err
	}
	if r.DeliveryStatus == "delivered" && r.ObservationError == "" && (r.Status == "succeeded" || r.Status == "failed" || r.Status == "cancelled") {
		return nil
	}
	if r.PendingNotification != nil {
		// An interrupted delivery is already frozen. Preserve its original arm,
		// payload and notifier even when a keyed caller arrives during recovery.
		var saved profileObserver
		if err := readProfileJSON(filepath.Join(dir, "observer.json"), &saved); err != nil {
			return err
		}
		return registerSharedObserver(ctx, dir, saved, token)
	}
	var saved profileObserver
	if e := readProfileJSON(filepath.Join(dir, "observer.json"), &saved); e == nil && cfg.Notify == "" {
		cfg.Notify = saved.Notify
	}
	if cfg.Notify == "" && (r.Status == "succeeded" || r.Status == "failed" || r.Status == "cancelled") {
		return nil
	}
	if defaultWake && cfg.Notify == "" {
		cfg.Notify = detectHarnessWake(notifierEnvironment())
		if cfg.Notify == "" {
			fmt.Fprintf(errOut, "wake: this shell exports no wake channel; to be woken when job %s finishes, run `myce wait %s` once as a background task (its exit wakes you)\n", r.JobID, r.JobID)
		}
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
	r.ArmNonce = rand.Text()
	r.DeliveryStatus = ""
	r.Delivery = "not_configured"
	if cfg.Notify != "" {
		r.Delivery = "armed"
	}
	if err = saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
		return err
	}
	if err := registerSharedObserver(ctx, dir, cfg, token); err != nil {
		r.Observation, r.ObservationError = "not_armed", "observer_registration"
		r.Delivery = "not_armed"
		if cfg.Notify == "" {
			r.Delivery = "not_configured"
		}
		if saveErr := saveProfileJSON(filepath.Join(dir, "receipt.json"), r); saveErr != nil {
			return errors.Join(err, saveErr)
		}
		return err
	}
	return readProfileJSON(filepath.Join(dir, "receipt.json"), r)
}

type profileNotification struct {
	ObservationError string            `json:"observation_error,omitempty"`
	DeliveryID       string            `json:"delivery_id"`
	JobID            string            `json:"job_id"`
	Key              string            `json:"key"`
	Status           string            `json:"status"`
	ResultReference  myce.FileManifest `json:"result_reference"`
}

// This is the detached myce wait process. The inherited flock survives caller
// exit and prevents another invocation from spawning a duplicate watcher.
func waitProfileReceipt(ctx context.Context, dir string, timeout time.Duration) error {
	syscall.CloseOnExec(3)
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
	j, waitErr := c.WaitSubmitted(ctx, &myce.Job{ID: r.JobID, State: r.Status, Spec: r.Spec})
	return finishProfileObservation(ctx, dir, r, cfg, j, waitErr)
}

func finishProfileObservation(ctx context.Context, dir string, r profileReceipt, cfg profileObserver, j *myce.Job, waitErr error, notifyEnv ...[]string) error {
	var notification profileNotification
	if r.PendingNotification != nil {
		notification = *r.PendingNotification
	} else {
		r.Observation = "not_armed"
		r.Delivery = "not_configured"
		if cfg.Notify != "" {
			r.Delivery = "not_armed"
		}
		notification = profileNotification{JobID: r.JobID, Key: r.Key}
		if waitErr != nil {
			r.ObservationError = "transport_error"
			if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(waitErr, context.DeadlineExceeded) {
				r.ObservationError = "timeout"
			} else if errors.Is(ctx.Err(), context.Canceled) || errors.Is(waitErr, context.Canceled) {
				r.ObservationError = "interrupted"
			} else if myce.IsTLSIdentityError(waitErr) {
				r.ObservationError = "tls_identity"
			}
			notification.Status, notification.ObservationError = "observation_failed", r.ObservationError
		} else {
			r.Status, r.ResultReference = j.State, j.OutputRefs
			r.FilesExpired = j.FilesExpired
			notification.Status, notification.ResultReference = r.Status, r.ResultReference
		}
		notification.DeliveryID = profileHash([]string{r.Nonce, r.Project, r.Key, r.JobID, notification.Status, notification.ObservationError})
		if waitErr != nil {
			notification.DeliveryID = profileHash([]string{notification.DeliveryID, r.ArmNonce})
		}
	}
	if cfg.Notify != "" {
		r.PendingNotification = &notification
		r.DeliveryStatus = "pending"
	}
	if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
		return err
	}
	if cfg.Notify != "" {
		b, _ := json.Marshal(notification)
		// Observation cancellation must still report its failure. The notifier owns
		// delivery deadlines/retries and gets no fleet token or diagnostic text.
		deliveryCtx, cancel := context.WithTimeout(context.Background(), observerNotifierBudget)
		defer cancel()
		env := notifierEnvironment()
		if len(notifyEnv) > 0 && notifyEnv[0] != nil {
			env = notifyEnv[0]
		}
		var deliveryErr error
		if isHarnessWake(cfg.Notify) {
			deliveryErr = deliverHarnessWake(deliveryCtx, cfg.Notify, env, notification)
		} else {
			cmd := exec.CommandContext(deliveryCtx, cfg.Notify)
			cmd.Stdin = strings.NewReader(string(b))
			cmd.Env = env
			deliveryErr = cmd.Run()
		}
		r.PendingNotification = nil
		r.DeliveryStatus = "delivered"
		if deliveryErr != nil {
			r.DeliveryStatus = "failed"
		}
		if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
			return err
		}
		if deliveryErr != nil {
			return deliveryErr
		}
	}
	if waitErr != nil {
		return waitError(2, "receipt observation ended; job continues; rerun the profile to recover")
	}
	return nil
}
