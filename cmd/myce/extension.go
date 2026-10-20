package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
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
	root := f.String("root", myce.DefaultExtensionRoot(), "local extension library and download cache")
	environmentRoot := f.String("environment-cache", "", "existing local environment cache for variant setup/check")
	environmentLimit := f.Int64("environment-cache-bytes", 20*1024*1024*1024, "environment cache limit")
	queue := f.String("queue", env("MYCE_QUEUE", "http://127.0.0.1:7331"), "queue address")
	token := f.String("token", env("MYCE_TOKEN", ""), "queue token (prefer MYCE_TOKEN)")
	// DefValue is only help text; keep the actual environment default private.
	f.Lookup("token").DefValue = ""
	static := f.Bool("static", false, "validate folder without executing or qualifying")
	host := f.String("host", "", "this host's configured worker host ID (local execution only)")
	digest := f.String("digest", "", "require exact extension SHA-256")
	local := f.Bool("local", false, "add locally without registering with the queue")
	platform := f.String("gpu", "", "qualification platform: cuda, xpu, metal or fake")
	device := f.String("device", "", "same selector as join --device")
	timeout := f.Duration("timeout", 10*time.Minute, "setup/check time limit")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := myce.Client{URL: *queue, Token: *token}
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
			if *token == "" {
				return errors.New("set MYCE_TOKEN or use --local")
			}
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
			fmt.Printf("PASS %s %s@%s\n", args[0], e.Name, e.Version)
			return nil
		}
		return errors.New("extension not installed")
	default:
		return errors.New("unknown extension command")
	}
}
