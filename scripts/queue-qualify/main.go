// Command queue-qualify measures whether a PostgreSQL server can hold the
// shared queue's authority (MY-40 stage 2): durable commit latency for the
// queue's real write shape, durability across restarts, isolation from a
// broken file mount and a returning member's catch-up cost.
//
// It is a separate Go module so a static binary built here runs on any fleet
// machine without adding qualification code or a driver to the myce binary.
// See docs/shared-queue-nas-qualification.md for the plan and thresholds.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

const usageText = `usage: queue-qualify COMMAND [flags]

Commands:
  info               server version, durability settings, identity and round trip
  commit             durable coordinator-turn renewals and ordered change appends
  durability-write   sequence-numbered durable writer with a local ledger
  durability-verify  check every acknowledged ledger entry against the server
  load               bounded bulk file I/O generator for one directory
  catchup            snapshot transfer and change-log replay into local SQLite
  summarize          combine JSON reports into one comparison
  cleanup            drop the qualification schema and leftover load directories

The connection string comes from the environment variable named by --dsn-env
(default MYCE_QUAL_PG_DSN) or from --dsn-file. It is never printed.
Run "queue-qualify COMMAND -h" for each command's flags.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

type command func(ctx context.Context, args []string, out io.Writer) error

var commands = map[string]command{
	"info":              cmdInfo,
	"commit":            cmdCommit,
	"durability-write":  cmdDurabilityWrite,
	"durability-verify": cmdDurabilityVerify,
	"load":              cmdLoad,
	"catchup":           cmdCatchup,
	"summarize":         cmdSummarize,
	"cleanup":           cmdCleanup,
}

// errChecksFailed reports a completed run whose report has a failing check.
var errChecksFailed = errors.New("one or more checks failed; see the report")

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Fprint(stderr, usageText)
		return 2
	}
	cmd, ok := commands[args[0]]
	if !ok {
		fmt.Fprintf(stderr, "queue-qualify: unknown command %q\n%s", args[0], usageText)
		return 2
	}
	err := cmd(ctx, args[1:], stdout)
	switch {
	case err == nil:
		return 0
	case errors.Is(err, flag.ErrHelp):
		return 2
	case errors.Is(err, errChecksFailed):
		fmt.Fprintf(stderr, "queue-qualify %s: %s\n", args[0], err)
		return 1
	default:
		// Every error passes through the active redactor, so a driver message
		// that names the host, user or database never reaches the terminal.
		fmt.Fprintf(stderr, "queue-qualify %s: %s\n", args[0], activeRedactor.text(err.Error()))
		return 1
	}
}

func newFlags(name string, out io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(out)
	return fs
}
