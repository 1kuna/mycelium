package main

import (
	"strings"

	"testing"
	"time"

	"mycelium/internal/myce"
)

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
