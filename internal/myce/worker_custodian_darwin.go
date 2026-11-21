package myce

import (
	"context"
	"encoding/xml"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

func custodianLabel(id string) string { return "local.mycelium.attempt." + id }
func custodianDomain() string         { return "gui/" + strconv.Itoa(os.Getuid()) }

func launchAttemptCustodian(ctx context.Context, c ServiceConfig, dir, id string, execution Execution) error {
	return launchDarwinCustodian(ctx, c, dir, id, &execution)
}

func launchCustodian(ctx context.Context, c ServiceConfig, dir, id string) error {
	return launchDarwinCustodian(ctx, c, dir, id, nil)
}

func launchDarwinCustodian(ctx context.Context, c ServiceConfig, dir, id string, execution *Execution) error {
	quote := func(s string) string {
		var out strings.Builder
		_ = xml.EscapeText(&out, []byte(s))
		return "<string>" + out.String() + "</string>"
	}
	args := ""
	for _, arg := range []string{c.Binary, "service", "--config", c.Config, "--attempt-custodian", id} {
		args += quote(arg)
	}
	log := filepath.Join(dir, "custodian-"+id+".log")
	stderr := log
	if execution != nil {
		stderr = filepath.Join(dir, "custodian-"+id+".stderr")
	}
	env := ""
	if session := os.Getenv(processSessionVariable); session != "" {
		env = "<key>EnvironmentVariables</key><dict><key>" + processSessionVariable + "</key>" + quote(session) + "</dict>"
	}
	plist := "<?xml version=\"1.0\" encoding=\"UTF-8\"?><plist version=\"1.0\"><dict><key>Label</key>" + quote(custodianLabel(id)) + "<key>ProgramArguments</key><array>" + args + "</array><key>RunAtLoad</key><true/><key>AbandonProcessGroup</key><true/><key>StandardOutPath</key>" + quote(log) + "<key>StandardErrorPath</key>" + quote(stderr) + env + "</dict></plist>"
	path := filepath.Join(dir, "custodian-"+id+".plist")
	if err := os.WriteFile(path, []byte(plist), 0600); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "launchctl", "bootstrap", custodianDomain(), path)
	cmd.Env = withoutMyceToken(os.Environ())
	out := &boundedProbeOutput{limit: custodianStartupOutputBytes}
	cmd.Stdout, cmd.Stderr = out, out
	err := cmd.Run()
	output := strings.TrimSpace(string(out.Bytes()))
	if execution == nil {
		if err != nil {
			return errors.New("custodian launchd bootstrap failed: " + output)
		}
		return waitCustodian(ctx, dir, id)
	}
	evidence := custodianStartupDiagnostic{ID: id, Execution: execution, Bootstrap: custodianLaunchctlResult(ctx, err), BootstrapOutput: output}
	if err != nil {
		observeCustodianStartupFailure(dir, evidence, false)
		return errors.New("custodian launchd bootstrap failed: " + evidence.BootstrapOutput)
	}
	saveCustodianStartupDiagnostic(dir, evidence)
	if err := waitCustodian(ctx, dir, id); err != nil {
		observeCustodianStartupFailure(dir, evidence, false)
		return err
	}
	return nil
}

func ownCustodianIsolation(id string) (string, error) {
	return "", verifyCustodianIsolation(id, os.Getpid(), "")
}

func verifyCustodianIsolation(id string, pid int, _ string) error {
	ctx, cancel := context.WithTimeout(context.Background(), processPSTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "launchctl", "print", custodianDomain()+"/"+custodianLabel(id)).Output()
	if err != nil {
		return errors.New("independent custodian launchd job unavailable")
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(line) == "pid = "+strconv.Itoa(pid) {
			return nil
		}
	}
	return errors.New("custodian launchd PID unconfirmed")
}

func retireCustodian(id string) {
	// Called only after the custodian has durably recorded payload completion
	// and verified release. Never remove a live execution to update its worker.
	ctx, cancel := context.WithTimeout(context.Background(), processPSTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "launchctl", "bootout", custodianDomain()+"/"+custodianLabel(id))
	cmd.Env = withoutMyceToken(os.Environ())
	_ = cmd.Run()
}
