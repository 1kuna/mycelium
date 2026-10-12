package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"mycelium/internal/myce"
)

func hostQueuesCommand(args []string, probe myce.MachineProbe, out io.Writer) error {
	if len(args) == 0 || (args[0] != "list" && args[0] != "forget") {
		return errors.New("usage: myce host-queues list|forget [--host-state-dir DIR] [--force] <queue-url>")
	}
	action := args[0]
	f := flag.NewFlagSet("host-queues "+action, flag.ContinueOnError)
	dir := f.String("host-state-dir", "", "shared local host control directory")
	force := f.Bool("force", false, "forget even when a local worker uses this queue")
	config := f.String("config", "", "installed worker config to check (also selects its host state directory)")
	// Accept the documented URL-first form as well as normal flag-first parsing.
	rest := args[1:]
	queue := ""
	if action == "forget" && len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
		queue = rest[0]
		rest = rest[1:]
	}
	if err := f.Parse(rest); err != nil {
		return err
	}
	if queue == "" && action == "forget" && f.NArg() == 1 {
		queue = f.Arg(0)
	} else if f.NArg() != 0 {
		return errors.New("unexpected host-queues arguments")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	if *config != "" {
		c, e := myce.ReadServiceConfig(*config)
		if e != nil {
			return e
		}
		if *dir == "" {
			*dir = c.HostStateDir
		}
	}
	if *dir == "" {
		*dir, err = myce.HostStateDirectory()
		if err != nil {
			return err
		}
	}
	h := myce.HostControl{Dir: *dir}
	if action == "list" {
		queues, e := h.Queues()
		if e != nil {
			return e
		}
		for _, q := range queues {
			fmt.Fprintln(out, q)
		}
		return nil
	}
	check := func(w myce.HostWorker) (bool, error) { return localWorkerUsesQueue(w, home, *config) }
	if err = h.ForgetQueue(queue, *force, check); err != nil {
		return err
	}
	fmt.Fprintln(out, "Forgot queue:", queue)
	return nil
}

func localWorkerUsesQueue(w myce.HostWorker, home, config string) (bool, error) {
	paths, err := filepath.Glob(filepath.Join(home, ".local", "share", "mycelium", "worker*", "config.json"))
	if err != nil {
		return false, err
	}
	paths = append(paths, filepath.Join(filepath.Dir(w.State), "config.json"))
	if w.Config != "" {
		paths = append(paths, w.Config)
	}
	if config != "" {
		paths = append(paths, config)
	}
	for _, path := range paths {
		c, e := myce.ReadServiceConfig(path)
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return false, e
		}
		if c.Role == "worker" && c.Queue == w.Queue {
			return true, nil
		}
	}
	// A live foreground worker is configured too; never steal its spool lock.
	lock, err := os.OpenFile(filepath.Join(w.State, "worker.lock"), os.O_RDWR, 0600)
	if err == nil {
		defer lock.Close()
		if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			if errors.Is(err, syscall.EWOULDBLOCK) {
				return true, nil
			}
			return false, err
		}
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	// Legacy manually installed units may have no saved service config. Refuse
	// unverifiable configured units; --force is the explicit override.
	if myce.ValidWorkerUnit(w.Unit) {
		path := filepath.Join(home, ".config", "systemd", "user", w.Unit)
		b, e := os.ReadFile(path)
		if e == nil {
			if strings.Contains(string(b), w.Queue) {
				return true, nil
			}
			if w.Config == "" {
				return false, errors.New("legacy worker unit configuration cannot be verified; use --config or --force")
			}
		} else if !errors.Is(e, os.ErrNotExist) {
			return false, e
		}
	}
	return false, nil
}
