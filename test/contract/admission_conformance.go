package contract

import (
	"context"
	"errors"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/contract/assert"
)

func RunAdmissionControllerConformance(t *testing.T, name string, newController func() ports.AdmissionController, job domain.Job, preset domain.Preset, claim domain.Claim) {
	t.Run(name+"/offer_commit_release", func(t *testing.T) {
		controller := newController()
		offer, err := controller.Offer(context.Background(), domain.AdmissionRequest{Job: job, Preset: preset, Claim: claim})
		assert.NoError(t, "Offer", err)
		assert.True(t, offer.OfferID != "", "offer id should be set: %+v", offer)
		assert.Equal(t, job.ID, offer.JobID, "offer job")
		assert.Equal(t, claim, offer.Claim, "offer claim")

		lease, err := controller.Commit(context.Background(), offer.OfferID, offer.Fence)
		assert.NoError(t, "Commit", err)
		assert.True(t, lease.ID != "", "lease id should be set: %+v", lease)
		assert.Equal(t, job.ID, lease.JobID, "lease job")
		assert.Equal(t, offer.NodeID, lease.NodeID, "lease node")
		assert.Equal(t, claim, lease.Claim, "lease claim")

		assert.NoError(t, "Release", controller.Release(context.Background(), lease.ID))
	})

	t.Run(name+"/commit_rejects_unknown_offer_and_stale_fence", func(t *testing.T) {
		controller := newController()
		_, err := controller.Commit(context.Background(), "missing-offer", 1)
		assert.Error(t, "Commit unknown offer", err)

		offer, err := controller.Offer(context.Background(), domain.AdmissionRequest{Job: job, Preset: preset, Claim: claim})
		assert.NoError(t, "Offer", err)
		_, err = controller.Commit(context.Background(), offer.OfferID, offer.Fence+1)
		assert.True(t, errors.Is(err, domain.ErrStaleFence), "stale Commit err = %v, want %v", err, domain.ErrStaleFence)
		lease, err := controller.Commit(context.Background(), offer.OfferID, offer.Fence)
		assert.NoError(t, "Commit after stale rejection", err)
		assert.NoError(t, "Release", controller.Release(context.Background(), lease.ID))
	})

	t.Run(name+"/direct_preempt_disabled", func(t *testing.T) {
		controller := newController()
		offer, err := controller.Offer(context.Background(), domain.AdmissionRequest{Job: job, Preset: preset, Claim: claim})
		assert.NoError(t, "Offer", err)
		lease, err := controller.Commit(context.Background(), offer.OfferID, offer.Fence)
		assert.NoError(t, "Commit", err)
		err = controller.Preempt(context.Background(), lease.ID, "conformance")
		assert.True(t, err != nil, "direct Preempt should be disabled")
		assert.NoError(t, "Release after disabled Preempt", controller.Release(context.Background(), lease.ID))
	})
}
