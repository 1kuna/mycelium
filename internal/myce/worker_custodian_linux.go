package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

func launchAttemptCustodian(ctx context.Context, c ServiceConfig, dir, id string, _ Execution) error {
	return launchCustodian(ctx, c, dir, id)
}

func launchCustodian(ctx context.Context, c ServiceConfig, dir, id string) error {
	if err := c.validateWorkerSlice(); err != nil {
		return err
	}
	log, err := os.OpenFile(filepath.Join(dir, "custodian-"+id+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer log.Close()
	// Scope placement happens before exec. The scope is a sibling of the worker
	// service, so KillMode=mixed and its final cgroup KILL cannot reach it.
	cmd := exec.Command("systemd-run", "--user", "--scope", "--collect", "--quiet", "--unit="+custodianUnit(id), "--slice="+c.custodianSlice(), "--property=Delegate=yes", c.Binary, "service", "--config", c.Config, "--attempt-custodian", id)
	cmd.Env = withoutMyceToken(os.Environ())
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		_ = RecordCustodianStartupFailure(c.State, id)
		return err
	}
	go func() {
		_ = cmd.Wait()
		_ = RecordCustodianStartupFailure(c.State, id)
	}()
	return waitCustodian(ctx, dir, id)
}

func ownCustodianIsolation(id string) (string, error) {
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "", err
	}
	return custodianCgroup(b, id)
}

func custodianCgroup(b []byte, id string) (string, error) {
	if !validCustodianID(id) {
		return "", errors.New("invalid detached attempt identity")
	}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(line, "0::/") {
			continue
		}
		path := filepath.Clean(strings.TrimPrefix(line, "0::"))
		if filepath.Base(path) == "worker" {
			path = filepath.Dir(path)
		}
		if filepath.Base(path) == custodianUnit(id) {
			return path, nil
		}
	}
	return "", errors.New("custodian is not in its independent systemd scope")
}

func verifyCustodianIsolation(id string, pid int, cgroup string) error {
	if filepath.Base(cgroup) != custodianUnit(id) {
		return errors.New("custodian cgroup identity mismatch")
	}
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cgroup")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(b)) != "0::"+filepath.Join(cgroup, "worker") {
		return errors.New("custodian scope membership unconfirmed")
	}
	return nil
}

func retireCustodian(string) {}

func retireAttemptCustodian(string, string, Result) bool { return false }
