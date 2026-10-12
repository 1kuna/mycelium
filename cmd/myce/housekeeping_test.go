package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestHostQueuesCLI(t *testing.T) {
	dir := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	w := myce.HostWorker{State: t.TempDir(), Name: "worker", Queue: "http://127.0.0.1:1111", Host: "local"}
	if err := myce.AdoptHostWorker(dir, w); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := hostQueuesCommand([]string{"list", "--host-state-dir", dir}, myce.MachineProbe{}, &out); err != nil || strings.TrimSpace(out.String()) != w.Queue {
		t.Fatalf("list failed: %v", err)
	}
	lock, err := os.OpenFile(filepath.Join(w.State, "worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	if err = hostQueuesCommand([]string{"forget", w.Queue, "--host-state-dir", dir}, myce.MachineProbe{}, &out); err == nil {
		t.Fatal("forgot live worker")
	}
	if err = hostQueuesCommand([]string{"forget", w.Queue, "--force", "--host-state-dir", dir}, myce.MachineProbe{}, &out); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err = hostQueuesCommand([]string{"list", "--host-state-dir", dir}, myce.MachineProbe{}, &out); err != nil || out.Len() != 0 {
		t.Fatal("forgotten queue still listed")
	}
}

func TestLocalWorkerQueueConfig(t *testing.T) {
	home := t.TempDir()
	root := t.TempDir()
	c, err := myce.PlanInstall("worker", home, root, t.TempDir(), "http://queue", "worker", myce.Detection{OS: "linux", Arch: "amd64", Platform: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
		t.Fatal(err)
	}
	// Config is non-secret and the reader needs no credentials.
	data := `{"role":"worker","queue":"http://queue"}`
	if err = os.WriteFile(c.Config, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	for _, queue := range []string{"http://queue", "http://old"} {
		used, e := localWorkerUsesQueue(myce.HostWorker{Config: c.Config, State: t.TempDir(), Queue: queue}, home, "")
		if e != nil || used != (queue == "http://queue") {
			t.Fatalf("configured queue check: used=%v err=%v", used, e)
		}
	}
}

func TestContainerRetentionInstallArgs(t *testing.T) {
	c := myce.ServiceConfig{Role: "worker", Config: "/config"}
	zero := time.Duration(0)
	c.ContainerRetention = &zero
	args := strings.Join(c.Args(), " ")
	if !strings.Contains(args, "--container-retention 0s") || !strings.Contains(args, "--service-config /config") {
		t.Fatal("installer dropped retention or config custody")
	}
	negative := -time.Second
	c.ContainerRetention = &negative
	if err := run([]string{"join", "--container-retention", "-1s", "--token", "fake", "--gpu", "fake", "--memory-gb", "1"}); err == nil || !strings.Contains(err.Error(), "container-retention") {
		t.Fatal("negative retention accepted")
	}
}
