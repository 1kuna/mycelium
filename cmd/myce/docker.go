package main

import (
	"context"
	"errors"
	"flag"
	"time"

	"mycelium/internal/myce"
)

// Exact offline custody reconciliation shares the host worker mutation boundary.
func resolveDockerCommand(args []string, probe myce.MachineProbe, resolver myce.HostIdentityResolver, launch myce.HostChildLauncher) error {
	f := flag.NewFlagSet("resolve-docker", flag.ContinueOnError)
	dir := f.String("state-dir", "", "offline registered worker spool")
	hostDir := f.String("host-state-dir", "", "shared local host control directory")
	identity := f.String("attempt-id", "", "exact attempt identity from worker custody")
	user := f.String("worker-user", "", "installed worker account (required under root)")
	config := f.String("config", "", "installed worker config")
	timeout := f.Duration("timeout", 10*time.Second, "exact-custody reconciliation deadline")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *timeout <= 0 || *dir == "" || *identity == "" {
		return errors.New("resolve-docker requires exact worker state, attempt identity and a positive timeout")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	v, err := resolver.Resolve(*user, *config, *hostDir)
	if err != nil {
		return err
	}
	_, err = v.Mutate(ctx, myce.HostMutation{Operation: "resolve-docker", SpoolDir: *dir, AttemptID: *identity, Timeout: *timeout}, launch, hostMutationExecutor(probe).Execute)
	return err
}
