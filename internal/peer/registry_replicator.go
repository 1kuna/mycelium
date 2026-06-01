package peer

import (
	"context"
	"errors"
	"fmt"

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

type RegistryReplicator struct {
	Local  ports.JobRegistry
	Peers  RegistryPeerSource
	Client RegistryClient
	SelfID string
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
		records, err := r.Client.Snapshot(ctx, peer)
		if err != nil {
			syncErr = errors.Join(syncErr, fmt.Errorf("pull registry from peer %q: %w", peer.ID, err))
			continue
		}
		for _, rec := range records {
			if err := r.Local.Put(ctx, rec); err != nil {
				return err
			}
		}
		local, err := r.Local.Snapshot(ctx)
		if err != nil {
			return err
		}
		if err := r.Client.Push(ctx, peer, redactPrivatePayloads(local)); err != nil {
			syncErr = errors.Join(syncErr, fmt.Errorf("push registry to peer %q: %w", peer.ID, err))
		}
	}
	return syncErr
}

func (r RegistryReplicator) PushRecord(ctx context.Context, rec domain.JobRecord) error {
	if err := r.validate(); err != nil {
		return err
	}
	peers, err := r.Peers.Peers(ctx)
	if err != nil {
		return err
	}
	var pushErr error
	for _, peer := range peers {
		if peer.ID == r.SelfID {
			continue
		}
		if err := r.Client.Push(ctx, peer, []domain.JobRecord{redactPrivatePayload(rec)}); err != nil {
			pushErr = errors.Join(pushErr, fmt.Errorf("push registry record to peer %q: %w", peer.ID, err))
		}
	}
	return pushErr
}

func (r RegistryReplicator) validate() error {
	if r.Local == nil || r.Peers == nil || r.Client == nil || r.SelfID == "" {
		return fmt.Errorf("registry replicator is not fully configured")
	}
	return nil
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
