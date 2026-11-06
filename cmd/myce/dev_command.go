package main

import (
	"context"
	"crypto/rand"
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

// A command-scoped dev session needs neither an installed fleet nor a second
// terminal. Its authority and state are private to this invocation.
func devCommandSession(ctx context.Context, args []string, in io.Reader, out, errOut io.Writer) (result error) {
	if len(args) == 0 {
		return errors.New("usage: myce dev -- <command> [args]")
	}
	root, err := os.MkdirTemp("", "myce-dev-")
	if err != nil {
		return err
	}
	token := rand.Text()
	d, err := myce.StartDev(myce.DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"),
		Checkpoints: filepath.Join(root, "checkpoints"), FileStore: filepath.Join(root, "files"),
		Token: token, GPU: "fake", Workers: 2, MemoryGB: 8})
	if err != nil {
		return errors.Join(err, os.RemoveAll(root))
	}
	defer func() {
		// Do not remove state unless all workers and the queue have joined.
		closeErr := d.Close()
		result = errors.Join(result, closeErr)
		if closeErr == nil {
			result = errors.Join(result, os.RemoveAll(root))
		}
	}()
	fmt.Fprintln(out, "Mycelium dev: temporary local queue · 2 fake workers · stops when the command exits")
	commandCtx, cancel := waitParentContext(ctx, os.Getppid(), time.Second, os.Getppid)
	defer cancel()
	cmd := exec.CommandContext(commandCtx, args[0], args[1:]...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = in, out, errOut
	// Never pass ambient fleet authority, callbacks or job context to this demo.
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "MYCE_") {
			cmd.Env = append(cmd.Env, value)
		}
	}
	cmd.Env = append(cmd.Env, "MYCE_QUEUE="+d.URL, "MYCE_TOKEN="+token)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// Cancellation must stop the command's descendants as well as its shell;
	// ordinary dev shutdown then handles any submitted jobs through exact release.
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second // Bound inherited pipes after a killed helper.
	if err := cmd.Start(); err != nil {
		return err
	}
	defer syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			code := 1
			var exited *exec.ExitError
			if errors.As(err, &exited) && exited.ExitCode() > 0 {
				code = exited.ExitCode()
			}
			if commandCtx.Err() != nil {
				code = 130
			}
			return &exitError{code, errors.New("dev command failed: " + strings.ReplaceAll(err.Error(), token, "[redacted]"))}
		}
		return nil
	case err := <-d.Errors():
		cancel()
		return errors.Join(err, <-done)
	}
}
