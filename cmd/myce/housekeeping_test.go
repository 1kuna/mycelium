package main

import (
	"os"
	"path/filepath"
	"strings"

	"testing"
	"time"

	"mycelium/internal/myce"
)

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
