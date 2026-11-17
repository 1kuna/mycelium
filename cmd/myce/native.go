package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"mycelium/internal/myce"
	"os"
	"time"
)

func resolveNativeCommand(args []string) error {
	f := flag.NewFlagSet("resolve-native", flag.ContinueOnError)
	dir := f.String("state-dir", "", "offline worker spool")
	identity := f.String("attempt-id", "", "exact attempt identity from worker custody")
	timeout := f.Duration("timeout", 30*time.Second, "inspection budget; expiry retains custody")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *dir == "" || *identity == "" || *timeout <= 0 {
		return errors.New("resolve-native requires exact worker state, attempt identity and a positive timeout")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	return myce.ReconcileNativeAttempt(ctx, *dir, *identity)
}

func resolveCustodianStartupCommand(args []string) error {
	f := flag.NewFlagSet("resolve-custodian-startup", flag.ContinueOnError)
	dir := f.String("state-dir", "", "offline worker spool")
	id := f.String("custodian-id", "", "exact detached owner identity from saved assignment")
	privileged := f.Bool("privileged-inspection", false, "use existing sudo -n authority for exact read-only Linux kernel handle inspection")
	timeout := f.Duration("timeout", 30*time.Second, "inspection budget; expiry retains custody")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *dir == "" || *id == "" || *timeout <= 0 {
		return errors.New("exact offline spool, custodian identity and positive timeout required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	return myce.ReconcileCustodianStartup(ctx, *dir, *id, *privileged)
}

// Invoked only by the opt-in recovery inspector. Runs before the ordinary root
// startup guard but exposes no queue, service/configuration or write operations.
func inspectCustodianHandlesCommand(args []string) error {
	f := flag.NewFlagSet("_inspect-custodian-handles", flag.ContinueOnError)
	dir := f.String("state-dir", "", "exact worker spool")
	id := f.String("custodian-id", "", "exact owner ID")
	uid := f.Int("owner-uid", 0, "ordinary owner UID")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected inspector arguments")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := myce.InspectCustodianHandles(ctx, *dir, *id, *uid); err != nil {
		return err
	}
	_, err := fmt.Fprintln(os.Stdout, "custodian handles clear: "+*id)
	return err
}
