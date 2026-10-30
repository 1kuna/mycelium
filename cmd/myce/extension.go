package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"mycelium/internal/myce"
)

func extensionCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: myce extension init|add|list|setup|check [flags] [folder|name@version]")
	}
	if args[0] == "init" {
		return extensionInit(args[1:])
	}
	f := flag.NewFlagSet("extension "+args[0], flag.ContinueOnError)
	root := f.String("root", myce.DefaultExtensionRoot(), "local extension library and download cache (default: installed worker root, then user cache)")
	environmentRoot := f.String("environment-cache", "", "existing local environment cache for variant setup/check (check default: installed worker cache)")
	environmentLimit := f.Int64("environment-cache-bytes", 0, "environment cache bytes (omitted: auto-size from cache disk)")
	queue := f.String("queue", env("MYCE_QUEUE", ""), "queue address")
	token := f.String("token", env("MYCE_TOKEN", ""), "queue token (prefer MYCE_TOKEN)")
	// DefValue is only help text; keep the actual environment default private.
	f.Lookup("token").DefValue = ""
	config := f.String("config", "", "installed config (default: installed worker, then queue)")
	joining := f.String("joining-file", env("MYCE_JOINING_FILE", ""), "privately supplied invitation (discovery plus saved address)")
	static := f.Bool("static", false, "validate folder without executing or qualifying")
	host := f.String("host", "", "this host's configured worker host ID (default: installed worker; local execution only)")
	digest := f.String("digest", "", "require exact extension SHA-256")
	local := f.Bool("local", false, "add locally without registering with the queue")
	platform := f.String("gpu", "", "qualification platform: cuda, xpu, metal or fake")
	device := f.String("device", "", "same selector as join --device")
	timeout := f.Duration("timeout", 10*time.Minute, "setup/check time limit")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	explicitRoot := false
	f.Visit(func(flag *flag.Flag) {
		if flag.Name == "root" {
			explicitRoot = true
		}
	})
	if !explicitRoot {
		if installed := installedExtensionRoot(*config); installed != "" {
			*root = installed
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Local operations remain usable without installed authority. Resolve the
	// shared pinned client only for operations that contact the queue.
	var client myce.Client
	if args[0] == "add" && !*local || args[0] == "check" && !*static {
		var err error
		client, err = runClient(*config, *joining, *queue, *token)
		if err != nil {
			return err
		}
	}
	switch args[0] {
	case "add":
		if f.NArg() != 1 {
			return errors.New("extension add requires a small folder")
		}
		e, err := myce.ReadExtension(f.Arg(0))
		if err != nil {
			return err
		}
		// Queue first: a frozen-version refusal must not leave a misleading local add.
		if !*local {
			if err := client.Call(ctx, "POST", "/api/extensions", e, nil); err != nil {
				return err
			}
		}
		if err := myce.InstallExtension(*root, e); err != nil {
			return err
		}
		fmt.Printf("added %s@%s %s\n", e.Name, e.Version, e.Digest)
		return nil
	case "list":
		if f.NArg() != 0 {
			return errors.New("extension list accepts no positional arguments")
		}
		extensions, err := myce.LocalExtensions(*root)
		if err != nil {
			return err
		}
		if *platform == "" {
			for i := range extensions {
				extensions[i].Files = nil
			}
			return json.NewEncoder(os.Stdout).Encode(extensions)
		}
		probe, cancel := context.WithTimeout(ctx, 15*time.Second)
		fingerprint, _ := myce.ExtensionFingerprint(probe, *platform, *device)
		cancel()
		for _, r := range myce.LocalExtensionReports(*root, *platform, fingerprint) {
			fmt.Println(myce.FormatExtensionReport(r))
		}
		return nil
	case "setup", "check":
		if args[0] == "check" && *static {
			if f.NArg() != 1 {
				return errors.New("check --static requires a folder")
			}
			e, err := myce.ReadExtension(f.Arg(0))
			if err != nil {
				return fmt.Errorf("FAIL static: %w", err)
			}
			if err := myce.StaticCheckExtension(e); err != nil {
				return fmt.Errorf("FAIL static: %w", err)
			}
			if *digest != "" && e.Digest != *digest {
				return errors.New("FAIL static: digest mismatch")
			}
			fmt.Printf("PASS static %s@%s %s (not qualified)\n", e.Name, e.Version, e.Digest)
			return nil
		}
		if args[0] == "check" {
			if worker, ok := installedExtensionWorker(*config); ok {
				if *host == "" {
					*host = worker.HostID
				}
				if *environmentRoot == "" {
					*environmentRoot = worker.EnvironmentCache
				}
				if *platform == "" || *platform != "cpu" && *device == "" {
					return errors.New("runtime check requires --gpu and (for GPUs) --device on an installed worker")
				}
			}
			if explicitRoot {
				warnExtensionCheckRoot(os.Stderr, *config, *root)
			}
			target, err := myce.ResolveExtensionCheckTarget(ctx, client, *host, *platform, *device, *environmentRoot)
			if err != nil {
				return err
			}
			*host, *platform, *device, *environmentRoot = target.Host, target.Platform, target.Device, target.EnvironmentCache
		}
		if f.NArg() != 1 || *platform == "" || *timeout <= 0 {
			return errors.New("setup/check requires --gpu, positive --timeout and name@version")
		}
		extensions, err := myce.LocalExtensions(*root)
		if err != nil {
			return err
		}
		for _, e := range extensions {
			if e.Name+"@"+e.Version != f.Arg(0) {
				continue
			}
			bounded, cancel := context.WithTimeout(ctx, *timeout)
			defer cancel()
			var cache *myce.EnvironmentCache
			if *environmentRoot != "" {
				cache = &myce.EnvironmentCache{Root: *environmentRoot, Limit: *environmentLimit}
			}
			if *digest != "" && e.Digest != *digest {
				return errors.New("FAIL: digest mismatch")
			}
			var reports []myce.ExtensionReport
			if args[0] == "check" {
				cache.Fetch = client.DownloadFile
				reports, err = myce.CheckExtension(bounded, *root, e, *platform, *device, *host, os.Stderr, cache, client)
			} else {
				reports, err = myce.PrepareExtension(bounded, *root, e, *platform, *device, false, os.Stderr, cache)
			}
			for _, r := range reports {
				fmt.Println(myce.FormatExtensionReport(r))
			}
			if err != nil {
				return fmt.Errorf("FAIL %s: %w", args[0], err)
			}
			if args[0] == "check" {
				for _, r := range reports {
					if !r.Qualified {
						return fmt.Errorf("check %s@%s: %s", e.Name, e.Version, r.Reason)
					}
				}
			}
			fmt.Printf("PASS %s %s@%s\n", args[0], e.Name, e.Version)
			return nil
		}
		return errors.New("extension not installed")
	default:
		return errors.New("unknown extension command")
	}
}

// Qualification reports belong to the root checked; they are not transferred
// to an installed worker. Inspect local configuration even with explicit queue
// credentials, which bypass installed authority resolution.
func warnExtensionCheckRoot(out io.Writer, config, root string) {
	workerRoot := installedExtensionRoot(config)
	if workerRoot == "" {
		return
	}
	checkRoot, err := filepath.Abs(root)
	if err != nil || checkRoot == workerRoot {
		return
	}
	// Relative paths and symlink aliases to the same directory are safe.
	checkInfo, checkErr := os.Stat(checkRoot)
	workerInfo, workerErr := os.Stat(workerRoot)
	if checkErr == nil && workerErr == nil && os.SameFile(checkInfo, workerInfo) {
		return
	}
	fmt.Fprintf(out, "WARNING: checking extension root %q; installed worker extension root is %q. The live worker is not qualified by this check. Rerun with --root %q to qualify the worker.\n", checkRoot, workerRoot, workerRoot)
}

// Read only local worker settings, without requiring queue credentials for
// offline extension commands. Installed workers use their spool's extensions.
func installedExtensionRoot(config string) string {
	c, ok := installedExtensionWorker(config)
	if !ok || c.State == "" {
		return ""
	}
	root, _ := filepath.Abs(filepath.Join(c.State, "extensions"))
	return root
}

func installedExtensionWorker(config string) (myce.ServiceConfig, bool) {
	if config == "" {
		home, _ := os.UserHomeDir()
		config = filepath.Join(home, ".local", "share", "mycelium", "worker", "config.json")
	}
	c, err := myce.ReadServiceConfig(config)
	return c, err == nil && c.Role == "worker"
}
