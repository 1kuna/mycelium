package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/url"
	"strings"
	"time"

	"mycelium/internal/myce"
)

func hostMaintenanceCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: myce host drain|disable|resume|status|budget HOST [--stop] [--memory-budget-gb GiB] [--wait] [--timeout DURATION]")
	}
	action := args[0]
	f := flag.NewFlagSet("host "+action, flag.ContinueOnError)
	address := f.String("queue", env("MYCE_QUEUE", "http://127.0.0.1:7331"), "queue address")
	token := f.String("token", "", "fleet token (prefer MYCE_TOKEN)")
	stop := f.Bool("stop", false, "disable and stop exact current attempts")
	wait := f.Bool("wait", false, "wait for fresh workers and zero known work/cleanup")
	timeout := f.Duration("timeout", 30*time.Second, "bounded wait; timeout never releases custody")
	cpuBudget := f.Int("cpu-budget-cores", 0, "queue-owned logical CPU allowance; zero uses detected capacity minus reserve")
	budget := f.Float64("memory-budget-gb", 0, "queue-owned host RAM allowance; requires drained/disabled host")
	var flags, hosts []string
	for i := 1; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "-") {
			flags = append(flags, arg)
			key := strings.TrimLeft(arg, "-")
			if (key == "queue" || key == "token" || key == "timeout" || key == "memory-budget-gb" || key == "cpu-budget-cores") && i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
		} else {
			hosts = append(hosts, arg)
		}
	}
	if err := f.Parse(flags); err != nil {
		return err
	}
	if len(hosts) != 1 || f.NArg() != 0 {
		return errors.New("exactly one physical HOST is required")
	}
	if action != "drain" && action != "disable" && action != "resume" && action != "status" && action != "budget" {
		return errors.New("unknown queue host action")
	}
	if *stop && action != "disable" {
		return errors.New("--stop requires disable")
	}
	if *timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	if *wait && (action == "resume" || action == "budget") {
		return errors.New("--wait requires drain, disable or status")
	}
	request := myce.HostMaintenanceRequest{Action: action, Stop: *stop}
	f.Visit(func(v *flag.Flag) {
		if v.Name == "cpu-budget-cores" {
			request.CPUBudgetCores = cpuBudget
		}
		if v.Name == "memory-budget-gb" {
			request.MemoryBudgetGB = budget
		}
	})
	if action == "status" && (request.MemoryBudgetGB != nil || request.CPUBudgetCores != nil) {
		return errors.New("status cannot change host policy")
	}
	if action == "budget" && request.MemoryBudgetGB == nil && request.CPUBudgetCores == nil {
		return errors.New("budget requires --memory-budget-gb GiB or --cpu-budget-cores N")
	}
	if *token == "" {
		*token = env("MYCE_TOKEN", "")
	}
	if *token == "" {
		return errors.New("set MYCE_TOKEN to the shared fleet token")
	}
	c := myce.Client{URL: *address, Token: *token}
	path := "/api/hosts/" + url.PathEscape(hosts[0]) + "/maintenance"
	var status myce.HostMaintenanceStatus
	method := "POST"
	var input any = request
	if action == "status" {
		method = "GET"
		input = nil
	}
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := c.Call(callCtx, method, path, input, &status)
	cancel()
	if err != nil {
		return err
	}
	if *wait {
		status, err = waitHostMaintenance(ctx, c, hosts[0], *timeout, myce.RealClock{}, waitPause)
	}
	if action == "status" && !*wait && !status.Healthy {
		err = errors.New("host health incomplete: missing, stale or inconsistent worker evidence")
	}
	if encodeErr := json.NewEncoder(out).Encode(status); encodeErr != nil {
		return encodeErr
	}
	return err
}

// Waiting only observes. Both deadline and pause are injectable; no timeout can
// discharge an attempt, a preparation or a pending daemon operation.
func waitHostMaintenance(ctx context.Context, c myce.Client, host string, timeout time.Duration, clock myce.Clock, pause func(context.Context, time.Duration) error) (myce.HostMaintenanceStatus, error) {
	deadline := clock.Now().Add(timeout)
	var status myce.HostMaintenanceStatus
	for {
		if err := ctx.Err(); err != nil {
			return status, err
		}
		remaining := deadline.Sub(clock.Now())
		if remaining <= 0 {
			return status, errors.New("host maintenance wait timed out; custody unchanged")
		}
		callCtx, cancel := context.WithTimeout(ctx, min(5*time.Second, remaining))
		err := c.Call(callCtx, "GET", "/api/hosts/"+url.PathEscape(host)+"/maintenance", nil, &status)
		cancel()
		if err != nil {
			return status, err
		}
		if status.Quiescent {
			return status, nil
		}
		if err = pause(ctx, min(time.Second, remaining)); err != nil {
			return status, err
		}
	}
}
