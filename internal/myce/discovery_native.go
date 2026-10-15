package myce

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/miekg/dns"
)

// Use the resident responder instead of competing for multicast sockets. A
// broken/missing client of a running daemon is an error, never a Go fallback.
func systemDiscoveryBackend() (string, error) {
	return discoveryBackend(runtime.GOOS, func() (bool, error) {
		conn, err := net.DialTimeout("unix", "/run/avahi-daemon/socket", 250*time.Millisecond)
		if err == nil {
			conn.Close()
			return true, nil
		}
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
			// The daemon can run with its simple-protocol socket disabled.
			if command, lookupErr := exec.LookPath("avahi-daemon"); lookupErr == nil {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				checkErr := exec.CommandContext(ctx, command, "--check").Run()
				if checkErr == nil {
					return true, nil
				}
				var exit *exec.ExitError
				if ctx.Err() != nil || !errors.As(checkErr, &exit) || exit.ExitCode() != 1 {
					return false, errors.New("cannot check Avahi daemon")
				}
			}
			return false, nil
		}
		return false, errors.New("cannot check Avahi daemon")
	})
}

func discoveryBackend(platform string, avahi func() (bool, error)) (string, error) {
	switch platform {
	case "darwin":
		return "bonjour", nil
	case "linux":
		running, err := avahi()
		if err != nil {
			return "", err
		}
		if running {
			return "avahi", nil
		}
	}
	return "", nil
}

type discoveryCommand func(context.Context, string, []string, func(string)) error

// No shell, credentials, or raw daemon diagnostics. Cancellation kills and
// reaps only this command; scanning continues until its pipe is closed.
func runDiscoveryCommand(ctx context.Context, name string, args []string, line func(string)) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.WaitDelay = time.Second
	for _, field := range os.Environ() {
		if !strings.HasPrefix(field, "MYCE_TOKEN=") && !strings.HasPrefix(field, "LC_ALL=") {
			cmd.Env = append(cmd.Env, field)
		}
	}
	cmd.Env = append(cmd.Env, "LC_ALL=C")
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return errors.New("cannot open discovery command output")
	}
	cmd.Stderr = cmd.Stdout
	if err = cmd.Start(); err != nil {
		return errors.New("cannot start native discovery command")
	}
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		line(scanner.Text())
	}
	scanErr := scanner.Err()
	if scanErr != nil {
		_ = cmd.Process.Kill()
	}
	err = cmd.Wait()
	if ctx.Err() != nil {
		return nil
	}
	if err != nil || scanErr != nil {
		return errors.New("native discovery command failed")
	}
	return nil
}

func discoveryAddress(ip string, port int) string {
	addr := net.ParseIP(ip)
	if addr == nil || port < 1 || port > 65535 || addr.IsUnspecified() || addr.IsMulticast() || addr.IsLinkLocalUnicast() {
		return ""
	}
	return "https://" + net.JoinHostPort(addr.String(), strconv.Itoa(port))
}

func browseNative(ctx context.Context, backend string, run discoveryCommand) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var mu sync.Mutex
	addresses := []string{}
	seen := map[string]bool{}
	add := func(ip string, port int) {
		address := discoveryAddress(ip, port)
		mu.Lock()
		defer mu.Unlock()
		if address != "" && !seen[address] && len(addresses) < 32 {
			seen[address] = true
			addresses = append(addresses, address)
		}
	}
	if backend == "avahi" {
		err := run(ctx, "avahi-browse", []string{"-pr", "-d", "local", discoveryService}, func(line string) {
			// Avahi's parsable form escapes embedded semicolons as decimal escapes.
			fields := strings.Split(line, ";")
			if len(fields) < 10 || fields[0] != "=" || fields[4] != discoveryService || fields[5] != "local" {
				return
			}
			compatible := false
			for _, txt := range strings.Fields(fields[9]) {
				if txt == `"version=1"` {
					compatible = true
				}
			}
			if !compatible {
				return
			}
			port, _ := strconv.Atoi(fields[8])
			add(fields[7], port)
		})
		return addresses, err
	}
	// dns-sd -Z performs browse + resolve and emits DNS zone records. Use the
	// existing DNS parser rather than parsing timestamps or human instance names.
	type service struct {
		target     string
		port       int
		compatible bool
		started    bool
	}
	services := map[string]*service{}
	var wg sync.WaitGroup
	var resolveErr error
	err := run(ctx, "/usr/bin/dns-sd", []string{"-Z", discoveryService, "local"}, func(line string) {
		record, err := dns.NewRR(line)
		if err != nil || record == nil {
			return
		}
		key := record.Header().Name
		if !strings.HasSuffix(key, "."+discoveryService+".") {
			return
		}
		s := services[key]
		if s == nil {
			if len(services) >= 32 {
				return
			}
			s = &service{}
			services[key] = s
		}
		switch rr := record.(type) {
		case *dns.SRV:
			s.target, s.port = rr.Target, int(rr.Port)
		case *dns.TXT:
			s.compatible = false
			for _, txt := range rr.Txt {
				if txt == "version=1" {
					s.compatible = true
				}
			}
		}
		if !s.compatible || s.target == "" || s.started {
			return
		}
		s.started = true
		target, port := s.target, s.port
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := run(ctx, "/usr/bin/dns-sd", []string{"-G", "v4v6", target}, func(line string) {
				fields := strings.Fields(line)
				if len(fields) < 7 || fields[1] != "Add" || strings.TrimSuffix(fields[4], ".") != strings.TrimSuffix(target, ".") {
					return
				}
				add(fields[5], port)
			})
			if e != nil {
				mu.Lock()
				resolveErr = e
				mu.Unlock()
			}
		}()
	})
	wg.Wait()
	return addresses, errors.Join(err, resolveErr)
}

func startDiscoveryAdvertisement(backend, command string, args []string, run discoveryCommand) (func(), error) {
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, command, args, func(line string) {
			if backend == "bonjour" && strings.Contains(line, "Got a reply for service ") && strings.Contains(line, "Name now registered and active") || backend == "avahi" && strings.HasPrefix(line, "Established under name '") {
				select {
				case ready <- struct{}{}:
				default:
				}
			}
		})
	}()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	var once sync.Once
	stop := func() { once.Do(func() { cancel(); <-done }) }
	select {
	case <-ready:
		return stop, nil
	case <-done:
		cancel()
		return nil, errors.New("native discovery advertisement exited before registration")
	case <-timer.C:
		stop()
		return nil, errors.New("native discovery advertisement registration timed out")
	}
}

func advertiseNative(address net.Addr, fingerprint, backend string, run discoveryCommand) (func(), error) {
	host, port, err := net.SplitHostPort(address.String())
	if err != nil {
		return nil, err
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 || len(fingerprint) < 16 {
		return nil, errors.New("invalid discovery advertisement")
	}
	ips := []net.IP{}
	if ip := net.ParseIP(host); ip != nil && !ip.IsUnspecified() {
		ips = append(ips, ip)
	} else {
		interfaces, err := net.Interfaces()
		if err != nil {
			return nil, err
		}
		for _, iface := range interfaces {
			if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagMulticast == 0 || iface.Flags&net.FlagLoopback != 0 {
				continue
			}
			addrs, err := iface.Addrs()
			if err != nil {
				return nil, err
			}
			for _, addr := range addrs {
				ip, _, err := net.ParseCIDR(addr.String())
				if err == nil && (host != "0.0.0.0" || ip.To4() != nil) && discoveryAddress(ip.String(), number) != "" {
					ips = append(ips, ip)
				}
			}
		}
	}
	stops := []func(){}
	stop := func() {
		for i := len(stops) - 1; i >= 0; i-- {
			stops[i]()
		}
	}
	seen := map[string]bool{}
	for _, ip := range ips {
		if seen[ip.String()] || discoveryAddress(ip.String(), number) == "" || len(seen) >= 32 {
			continue
		}
		seen[ip.String()] = true
		// One opaque proxy per address preserves multihomed/IPv6 listeners without
		// advertising the user's machine name. Each is authenticated independently.
		name := "myce-" + fingerprint[:16] + "-" + strconv.Itoa(len(seen))
		hostname := name + ".local."
		var withdraw func()
		if backend == "bonjour" {
			args := []string{"-P", name, discoveryService, "local", port, hostname, ip.String(), "version=1"}
			if ip.IsLoopback() {
				args = append([]string{"-lo"}, args...)
			}
			withdraw, err = startDiscoveryAdvertisement(backend, "/usr/bin/dns-sd", args, run)
		} else {
			withdraw, err = startDiscoveryAdvertisement(backend, "avahi-publish-address", []string{"-R", hostname, ip.String()}, run)
			if err == nil {
				stops = append(stops, withdraw)
				withdraw, err = startDiscoveryAdvertisement(backend, "avahi-publish-service", []string{"-H", hostname, name, discoveryService, port, "version=1"}, run)
			}
		}
		if err != nil {
			stop()
			return nil, err
		}
		stops = append(stops, withdraw)
	}
	if len(stops) == 0 {
		return nil, errors.New("no usable discovery address")
	}
	var once sync.Once
	return func() { once.Do(stop) }, nil
}
