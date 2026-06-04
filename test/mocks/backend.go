package mocks

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type BackendCall struct {
	Op     string
	Addr   string
	Preset domain.Preset
}

type BackendAdapter struct {
	NameVal    string
	LaunchErr  error
	StopErr    error
	ReadyAfter int
	readyCalls int
	Calls      []BackendCall
}

func NewBackendAdapter() *BackendAdapter {
	return &BackendAdapter{NameVal: "mock"}
}

func (m *BackendAdapter) Name() string {
	return m.NameVal
}

func (m *BackendAdapter) Launch(_ context.Context, p domain.Preset, addr string) (ports.Handle, error) {
	m.Calls = append(m.Calls, BackendCall{Op: "launch", Addr: addr, Preset: p})
	if m.LaunchErr != nil {
		return ports.Handle{}, m.LaunchErr
	}
	return ports.Handle{PID: 4242, Addr: addr, Kind: "process", Ref: "4242"}, nil
}

func (m *BackendAdapter) LaunchDynamic(ctx context.Context, p domain.Preset, addr string) (ports.Handle, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return ports.Handle{}, err
	}
	if port == "0" {
		port = strconv.Itoa(10000 + len(m.Calls))
	}
	if host == "" {
		host = "127.0.0.1"
	}
	concrete := net.JoinHostPort(host, port)
	m.Calls = append(m.Calls, BackendCall{Op: "launch_dynamic", Addr: concrete, Preset: p})
	if m.LaunchErr != nil {
		return ports.Handle{}, m.LaunchErr
	}
	return ports.Handle{PID: 4242, Addr: concrete, Kind: "process", Ref: "4242"}, nil
}

func (m *BackendAdapter) WaitReady(ctx context.Context, addr string) error {
	m.Calls = append(m.Calls, BackendCall{Op: "waitready", Addr: addr})
	if err := ctx.Err(); err != nil {
		return err
	}
	m.readyCalls++
	if m.readyCalls <= m.ReadyAfter {
		return fmt.Errorf("%w: not ready yet (call %d)", domain.ErrNotReady, m.readyCalls)
	}
	return nil
}

func (m *BackendAdapter) Stop(_ context.Context, h ports.Handle) error {
	m.Calls = append(m.Calls, BackendCall{Op: "stop", Addr: h.Addr})
	return m.StopErr
}

var _ ports.BackendAdapter = (*BackendAdapter)(nil)
var _ ports.DynamicBackendAdapter = (*BackendAdapter)(nil)
