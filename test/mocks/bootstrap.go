package mocks

import (
	"context"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type HostDetector struct {
	Host  domain.HostFacts
	Err   error
	Calls []domain.Node
}

func (m *HostDetector) DetectHost(_ context.Context, seed domain.Node) (domain.HostFacts, error) {
	m.Calls = append(m.Calls, seed)
	if m.Err != nil {
		return domain.HostFacts{}, m.Err
	}
	return m.Host, nil
}

type EngineDetector struct {
	Profiles []domain.EngineProfile
	Err      error
	Calls    []domain.HostFacts
}

func (m *EngineDetector) DetectEngines(_ context.Context, host domain.HostFacts) ([]domain.EngineProfile, error) {
	m.Calls = append(m.Calls, host)
	if m.Err != nil {
		return nil, m.Err
	}
	return append([]domain.EngineProfile(nil), m.Profiles...), nil
}

type BootstrapPlanner struct {
	Plan  domain.BootstrapPlan
	Err   error
	Calls []domain.BootstrapRequest
}

func (m *BootstrapPlanner) PlanBootstrap(_ context.Context, req domain.BootstrapRequest, _ domain.HostFacts, _ []domain.EngineProfile) (domain.BootstrapPlan, error) {
	m.Calls = append(m.Calls, req)
	if m.Err != nil {
		return domain.BootstrapPlan{}, m.Err
	}
	return m.Plan, nil
}

type EngineInstaller struct {
	Result domain.BootstrapResult
	Err    error
	Calls  []domain.BootstrapPlan
}

func (m *EngineInstaller) ApplyBootstrapPlan(_ context.Context, plan domain.BootstrapPlan, progress func(domain.BootstrapEvent)) (domain.BootstrapResult, error) {
	m.Calls = append(m.Calls, plan)
	if m.Err != nil {
		return domain.BootstrapResult{}, m.Err
	}
	for _, event := range m.Result.Events {
		if progress != nil {
			progress(event)
		}
	}
	return m.Result, nil
}

type EngineVerifier struct {
	Verification domain.EngineVerification
	Err          error
	Calls        []domain.EngineProfile
}

func (m *EngineVerifier) VerifyEngine(_ context.Context, profile domain.EngineProfile) (domain.EngineVerification, error) {
	m.Calls = append(m.Calls, profile)
	if m.Err != nil {
		return domain.EngineVerification{}, m.Err
	}
	return m.Verification, nil
}

type EngineRegistry struct {
	Profiles []domain.EngineProfile
	Err      error
}

func (m *EngineRegistry) SaveEngineProfile(_ context.Context, profile domain.EngineProfile) error {
	if m.Err != nil {
		return m.Err
	}
	m.Profiles = append(m.Profiles, profile)
	return nil
}

func (m *EngineRegistry) ListEngineProfiles(context.Context) ([]domain.EngineProfile, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	return append([]domain.EngineProfile(nil), m.Profiles...), nil
}

func (m *EngineRegistry) MarkEngineProfileUnready(_ context.Context, profileID, reason string) error {
	if m.Err != nil {
		return m.Err
	}
	for i := range m.Profiles {
		if m.Profiles[i].ID == profileID {
			m.Profiles[i].Ready = false
			m.Profiles[i].UnreadyReason = reason
			return nil
		}
	}
	m.Profiles = append(m.Profiles, domain.EngineProfile{ID: profileID, Ready: false, UnreadyReason: reason})
	return nil
}

var _ ports.HostDetector = (*HostDetector)(nil)
var _ ports.EngineDetector = (*EngineDetector)(nil)
var _ ports.BootstrapPlanner = (*BootstrapPlanner)(nil)
var _ ports.EngineInstaller = (*EngineInstaller)(nil)
var _ ports.EngineVerifier = (*EngineVerifier)(nil)
var _ ports.EngineRegistry = (*EngineRegistry)(nil)
