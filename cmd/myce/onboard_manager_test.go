package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type enrollmentTestChild struct {
	cmd  *exec.Cmd
	done chan struct{}
}

// The test process is the worker's parent and owns Wait, even across failed
// installs and rollbacks. Short-lived shell manager commands never own workers.
type enrollmentTestManager struct {
	mu       sync.Mutex
	home     string
	current  *enrollmentTestChild
	children []*enrollmentTestChild
}

func newEnrollmentTestManager(t *testing.T, home string) *enrollmentTestManager {
	t.Helper()
	m := &enrollmentTestManager{home: home}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		switch r.URL.Path {
		case "/start":
			pid, err := m.start()
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			fmt.Fprintln(w, pid)
		case "/stop":
			if err := m.stop(); err != nil {
				http.Error(w, err.Error(), 500)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(func() {
		server.Close()
		m.mu.Lock()
		defer m.mu.Unlock()
		if err := m.stop(); err != nil {
			t.Error(err)
		}
		for _, child := range m.children {
			select {
			case <-child.done:
				if child.cmd.ProcessState == nil {
					t.Error("fake worker was not joined")
				}
			default:
				t.Error("fake manager left a child running")
			}
		}
	})
	if err := os.WriteFile(filepath.Join(home, "fake-manager.url"), []byte(server.URL), 0600); err != nil {
		t.Fatal(err)
	}
	return m
}

func (m *enrollmentTestManager) start() (int, error) {
	if m.current != nil {
		select {
		case <-m.current.done:
			m.current = nil
		default:
			return m.current.cmd.Process.Pid, nil // enable --now is idempotent.
		}
	}
	base := filepath.Join(m.home, ".local", "share", "mycelium", "worker")
	cmd := exec.Command(filepath.Join(base, "myce"), "service", "--config", filepath.Join(base, "config.json"))
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key != "HOME" && key != "PATH" && key != "MYCE_TOKEN" && key != "MYCE_QUEUE" && key != "MYCE_JOINING_FILE" {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "HOME="+m.home, "PATH="+filepath.Join(m.home, "bin")+":/usr/bin:/bin:/usr/sbin:/sbin")
	log, err := os.OpenFile(filepath.Join(m.home, "fake-service.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return 0, err
	}
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		return 0, err
	}
	child := &enrollmentTestChild{cmd: cmd, done: make(chan struct{})}
	m.current = child
	m.children = append(m.children, child)
	go func() {
		_ = cmd.Wait()
		_ = log.Close()
		close(child.done)
	}()
	return cmd.Process.Pid, nil
}

func (m *enrollmentTestManager) stop() error {
	child := m.current
	if child == nil {
		return nil
	}
	select {
	case <-child.done:
		m.current = nil
		return nil
	default:
	}
	err := child.cmd.Process.Signal(syscall.SIGTERM)
	if errors.Is(err, os.ErrProcessDone) {
		err = nil
	}
	select {
	case <-child.done:
		m.current = nil
		return err
	case <-time.After(10 * time.Second):
		// Kill is a bounded fallback, never proof of exit: still join Wait.
		killErr := child.cmd.Process.Kill()
		select {
		case <-child.done:
			m.current = nil
		case <-time.After(5 * time.Second):
			return errors.Join(err, killErr, errors.New("fake worker did not join after kill"))
		}
		return errors.Join(err, killErr, errors.New("fake worker exceeded graceful shutdown budget"))
	}
}

func TestEnrollmentFakeManagerJoinsEveryWorker(t *testing.T) {
	home := t.TempDir()
	base := filepath.Join(home, ".local", "share", "mycelium", "worker")
	if err := os.MkdirAll(base, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "myce"), []byte("#!/bin/sh\nexec sleep 60\n"), 0700); err != nil {
		t.Fatal(err)
	}
	m := newEnrollmentTestManager(t, home)
	first, err := m.start()
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.start()
	if err != nil || first != second {
		t.Fatal("manager duplicated a running worker", first, second, err)
	}
	if err := m.stop(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.start(); err != nil {
		t.Fatal(err)
	}
	if err := m.stop(); err != nil {
		t.Fatal(err)
	}
	if len(m.children) != 2 {
		t.Fatal("missing restart", len(m.children))
	}
	for _, child := range m.children {
		select {
		case <-child.done:
			if child.cmd.ProcessState == nil {
				t.Fatal("worker was not joined")
			}
		default:
			t.Fatal("worker still running")
		}
	}
}
