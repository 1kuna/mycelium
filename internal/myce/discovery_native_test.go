package myce

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiscoveryUsesResidentDaemon(t *testing.T) {
	for _, tc := range []struct {
		platform string
		running  bool
		probeErr error
		backend  string
		wantErr  bool
	}{
		{"darwin", false, errors.New("must not probe Avahi"), "bonjour", false},
		{"linux", true, nil, "avahi", false},
		{"linux", false, nil, "", false},
		{"linux", false, errors.New("permission denied"), "", true},
		{"windows", false, nil, "", false},
	} {
		got, err := discoveryBackend(tc.platform, func() (bool, error) { return tc.running, tc.probeErr })
		if got != tc.backend || (err != nil) != tc.wantErr {
			t.Fatalf("%s: backend=%s err=%v", tc.platform, got, err)
		}
	}
}

func TestNativeBrowseResolvesCompatibleAddresses(t *testing.T) {
	for _, backend := range []string{"bonjour", "avahi"} {
		t.Run(backend, func(t *testing.T) {
			run := func(ctx context.Context, command string, args []string, line func(string)) error {
				switch command {
				case "avahi-browse":
					for _, record := range []string{
						`=;en0;IPv4;myce-opaque;_mycelium._tcp;local;opaque.local;192.0.2.1;17342;"version=1"`,
						`=;en0;IPv6;myce-opaque;_mycelium._tcp;local;opaque.local;2001:db8::1;17342;"version=1"`,
						`=;en0;IPv4;old;_mycelium._tcp;local;opaque.local;192.0.2.2;17342;"version=2"`,
						`=;en0;IPv6;local;_mycelium._tcp;local;opaque.local;fe80::1;17342;"version=1"`,
						`=;en0;IPv4;bad;_mycelium._tcp;local;opaque.local;192.0.2.3;0;"version=1"`,
					} {
						line(record)
					}
				case "/usr/bin/dns-sd":
					if args[0] == "-Z" {
						for _, record := range []string{
							``,
							`; dns-sd zone-file comment`,
							`myce-opaque._mycelium._tcp SRV 0 0 17342 opaque.local.`,
							`myce-opaque._mycelium._tcp TXT "version=1"`,
							`myce-opaque._mycelium._tcp TXT "version=1"`,
							`old._mycelium._tcp TXT "version=2"`,
							`old._mycelium._tcp SRV 0 0 17342 old.local.`,
							`garbage`,
						} {
							line(record)
						}
					} else if args[0] == "-G" && args[2] == "opaque.local." {
						line(`12:00:00.000 Add 2 6 opaque.local. 192.0.2.1 240`)
						line(`12:00:00.000 Add 2 6 opaque.local. 2001:db8::1 240`)
						line(`12:00:00.000 Add 2 6 opaque.local. fe80::1%en0 240`)
						line(`12:00:00.000 Rmv 0 6 opaque.local. 192.0.2.2 0`)
					} else {
						t.Errorf("unexpected resolution: %v", args)
					}
				default:
					t.Errorf("unexpected command %s", command)
				}
				return nil
			}
			addresses, err := browseNative(context.Background(), backend, run)
			if err != nil || len(addresses) != 2 || addresses[0] != "https://192.0.2.1:17342" || addresses[1] != "https://[2001:db8::1]:17342" {
				t.Fatal(addresses, err)
			}
		})
	}
}

func TestNativeAdvertisementRegistrationAndCleanup(t *testing.T) {
	for _, backend := range []string{"bonjour", "avahi"} {
		t.Run(backend, func(t *testing.T) {
			var starts, stops atomic.Int32
			run := func(ctx context.Context, command string, args []string, line func(string)) error {
				starts.Add(1)
				joined := strings.Join(args, " ")
				if !strings.Contains(joined, "myce-0123456789abcdef-1") || strings.Contains(joined, "private-token") {
					t.Error(joined)
				}
				if backend == "bonjour" {
					if command != "/usr/bin/dns-sd" || args[0] != "-lo" || !strings.Contains(joined, "-P") {
						t.Error(command, args)
					}
					line("Got a reply for service myce-opaque: Name now registered and active")
				} else {
					line("Established under name 'myce-opaque'")
				}
				<-ctx.Done()
				stops.Add(1)
				return nil
			}
			withdraw, err := advertiseNative(&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 17342}, "0123456789abcdef0123456789abcdef", backend, run)
			if err != nil {
				t.Fatal(err)
			}
			withdraw()
			withdraw()
			want := int32(1)
			if backend == "avahi" {
				want = 2
			}
			if starts.Load() != want || stops.Load() != want {
				t.Fatal(starts.Load(), stops.Load())
			}
		})
	}
}

func TestNativeAdvertisementRollsBackPartialRegistration(t *testing.T) {
	var stopped atomic.Bool
	run := func(ctx context.Context, command string, args []string, line func(string)) error {
		if command == "avahi-publish-service" {
			return errors.New("registration rejected")
		}
		line("Established under name 'myce-opaque'")
		<-ctx.Done()
		stopped.Store(true)
		return nil
	}
	_, err := advertiseNative(&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 17342}, "0123456789abcdef0123456789abcdef", "avahi", run)
	if err == nil || !stopped.Load() {
		t.Fatal(err, stopped.Load())
	}
}

func TestDiscoveryCommandCancellationAndCredentialIsolation(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "private-token")
	script := filepath.Join(t.TempDir(), "discovery-command")
	if err := os.WriteFile(script, []byte("#!/bin/sh\n[ -z \"$MYCE_TOKEN\" ] || exit 9\n[ \"$LC_ALL\" = C ] || exit 8\nprintf 'ready\\n'\nexec sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ready := false
	err := runDiscoveryCommand(ctx, script, nil, func(line string) {
		if line == "ready" {
			ready = true
			cancel()
		}
	})
	if err != nil || !ready {
		t.Fatal(err, ready)
	}
	if err = runDiscoveryCommand(context.Background(), "/does-not-exist", nil, func(string) {}); err == nil {
		t.Fatal("missing native tool hidden")
	}
}
