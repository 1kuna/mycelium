package peer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type RegistryPeerSource interface {
	Peers(ctx context.Context) ([]domain.Peer, error)
}

type RegistryClient interface {
	Snapshot(ctx context.Context, peer domain.Peer) ([]domain.JobRecord, error)
	Push(ctx context.Context, peer domain.Peer, records []domain.JobRecord) error
}

type RescueSnapshotter interface {
	RescueSnapshot(ctx context.Context) ([]domain.JobRecord, error)
}

type RegistryReplicator struct {
	Local   ports.JobRegistry
	Peers   RegistryPeerSource
	Client  RegistryClient
	SelfID  string
	Timeout time.Duration
}

func RescueSnapshot(records []domain.JobRecord) []domain.JobRecord {
	out := make([]domain.JobRecord, 0, len(records))
	for _, rec := range records {
		if terminalRecordStatus(rec.Status) && !rec.CleanupRequired {
			continue
		}
		out = append(out, cloneRecord(rec))
	}
	return out
}

func (r RegistryReplicator) SyncOnce(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	peers, err := r.Peers.Peers(ctx)
	if err != nil {
		return err
	}
	var syncErr error
	for _, peer := range peers {
		if peer.ID == r.SelfID {
			continue
		}
		peerCtx, cancel := r.peerContext(ctx)
		records, err := r.Client.Snapshot(peerCtx, peer)
		cancel()
		if err != nil {
			syncErr = errors.Join(syncErr, fmt.Errorf("pull registry from peer %q: %w", peer.ID, err))
			continue
		}
		for _, rec := range records {
			if err := r.Local.Put(ctx, rec); err != nil {
				return err
			}
		}
		local, err := RescueRecords(ctx, r.Local)
		if err != nil {
			return err
		}
		peerCtx, cancel = r.peerContext(ctx)
		err = r.Client.Push(peerCtx, peer, redactPrivatePayloads(local))
		cancel()
		if err != nil {
			syncErr = errors.Join(syncErr, fmt.Errorf("push registry to peer %q: %w", peer.ID, err))
		}
	}
	return syncErr
}

func (r RegistryReplicator) PushRecord(ctx context.Context, rec domain.JobRecord) error {
	if err := r.validate(); err != nil {
		return err
	}
	if terminalRecordStatus(rec.Status) && !rec.CleanupRequired {
		return nil
	}
	peers, err := r.Peers.Peers(ctx)
	if err != nil {
		return err
	}
	var pushErr error
	attempted := 0
	succeeded := 0
	for _, peer := range peers {
		if peer.ID == r.SelfID {
			continue
		}
		attempted++
		peerCtx, cancel := r.peerContext(ctx)
		err := r.Client.Push(peerCtx, peer, []domain.JobRecord{redactPrivatePayload(rec)})
		cancel()
		if err != nil {
			pushErr = errors.Join(pushErr, fmt.Errorf("push registry record to peer %q: %w", peer.ID, err))
			continue
		}
		succeeded++
	}
	if attempted == 0 {
		return fmt.Errorf("no reachable registry peer for rescue copy")
	}
	if succeeded == 0 {
		return fmt.Errorf("no registry rescue copy reached a peer: %w", pushErr)
	}
	return nil
}

func (r RegistryReplicator) validate() error {
	if r.Local == nil || r.Peers == nil || r.Client == nil || r.SelfID == "" {
		return fmt.Errorf("registry replicator is not fully configured")
	}
	return nil
}

func (r RegistryReplicator) peerContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if r.Timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, r.Timeout)
}

func RescueRecords(ctx context.Context, registry ports.JobRegistry) ([]domain.JobRecord, error) {
	if rescue, ok := registry.(RescueSnapshotter); ok {
		return rescue.RescueSnapshot(ctx)
	}
	return nil, fmt.Errorf("job registry does not support rescue snapshot")
}

func redactPrivatePayloads(records []domain.JobRecord) []domain.JobRecord {
	out := make([]domain.JobRecord, len(records))
	for i, rec := range records {
		out[i] = redactPrivatePayload(rec)
	}
	return out
}

func redactPrivatePayload(rec domain.JobRecord) domain.JobRecord {
	if rec.Handling != domain.HandlingPrivate {
		return rec
	}
	rec.Request = nil
	rec.PayloadRedacted = true
	return rec
}
