package myce

import (
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestInstallSystemdUnavailableTimeout(t *testing.T) {
	for _, tc := range []struct {
		name, properties string
		err              error
	}{
		{name: "empty"},
		{name: "malformed", properties: "TimeoutStopUSec=unrecognized\nTimeoutStartUSec=90s\n"},
		{name: "missing-start", properties: "TimeoutStopUSec=45s\n"},
		{name: "unsupported-query", err: errors.New("unsupported property")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var warning testLogBuffer
			previous := log.Writer()
			log.SetOutput(&warning)
			defer log.SetOutput(previous)
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls := 0
				command := BoundSetupCommand(func(got context.Context, _ string, args ...string) ([]byte, error) {
					if contains(args, "show") {
						return []byte(tc.properties), tc.err
					}
					calls++
					if _, ok := got.Deadline(); ok {
						t.Fatal("unknown manager deadline replaced by a guessed deadline")
					}
					if calls == 1 {
						select {
						case <-time.After(20 * time.Second):
							return nil, nil
						case <-got.Done():
							return nil, got.Err()
						}
					}
					cancel()
					<-got.Done()
					return nil, got.Err()
				})
				if _, err := command(ctx, "systemctl", "--user", "restart", "fixture.service"); err != nil {
					t.Fatalf("slow lifecycle should complete: %v", err)
				}
				if _, err := command(ctx, "systemctl", "--user", "restart", "fixture.service"); !errors.Is(err, context.Canceled) {
					t.Fatalf("caller cancellation lost: %v", err)
				}
				if calls != 2 || !strings.Contains(warning.String(), "warning: effective systemd service timeout") {
					t.Fatalf("missing lifecycle calls or fallback warning: calls=%d log=%q", calls, warning.String())
				}
			})
		})
	}
}

func TestInstallLaunchdLifecycleWait(t *testing.T) {
	for _, action := range []string{"bootout", "bootstrap", "kickstart", "kill", "print"} {
		t.Run(action, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if action == "print" {
					ctx = context.WithValue(ctx, launchdLifecycleKey{}, true)
				}
				command := BoundSetupCommand(func(got context.Context, name string, args ...string) ([]byte, error) {
					if name != "launchctl" || args[0] != action {
						t.Fatalf("unexpected timeout query: %s %v", name, args)
					}
					select {
					case <-time.After(20 * time.Second):
					case <-got.Done():
						t.Fatalf("lifecycle killed by setup deadline: %v", got.Err())
					}
					cancel()
					<-got.Done()
					return nil, got.Err()
				})
				if _, err := command(ctx, "launchctl", action, "fixture"); !errors.Is(err, context.Canceled) {
					t.Fatalf("caller cancellation lost: %v", err)
				}
			})
		})
	}
}

func TestInstallSystemdEffectiveLifecycleBudgets(t *testing.T) {
	for _, tc := range []struct {
		action []string
		budget time.Duration
	}{{[]string{"stop"}, 45 * time.Second}, {[]string{"start"}, 135 * time.Second}, {[]string{"restart"}, 135 * time.Second}, {[]string{"enable", "--now"}, 135 * time.Second}, {[]string{"disable", "--now"}, 45 * time.Second}} {
		t.Run(strings.Join(tc.action, "-"), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				command := BoundSetupCommand(func(ctx context.Context, _ string, args ...string) ([]byte, error) {
					if contains(args, "show") {
						time.Sleep(time.Second)
						return []byte("TimeoutStopUSec=45s\nTimeoutStartUSec=1min 30s\n"), nil
					}
					deadline, ok := ctx.Deadline()
					if !ok || deadline.Sub(time.Now()) != tc.budget+2*time.Second {
						t.Fatalf("effective manager budget/margin lost: %s", deadline.Sub(time.Now()))
					}
					return nil, nil
				})
				if _, err := command(context.Background(), "systemctl", append(append([]string{"--user"}, tc.action...), "fixture.service")...); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
	if d, err := systemdTimeout("1d 2h 3min 4s 5ms 6us"); err != nil || d != 26*time.Hour+3*time.Minute+4*time.Second+5*time.Millisecond+6*time.Microsecond {
		t.Fatalf("systemd formatted timespan: %s %v", d, err)
	}
}
