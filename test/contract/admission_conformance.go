package contract

import (
	"context"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/contract/assert"
)

func RunAdmissionControllerConformance(t *testing.T, name string, newController func() ports.AdmissionController, job domain.Job, claim domain.Claim) {
	t.Run(name+"/offer_commit_release", func(t *testing.T) {
		controller := newController()
		offer, err := controller.Offer(context.Background(), domain.AdmissionRequest{Job: job, Claim: claim})
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

	t.Run(name+"/offer_commit_preempt", func(t *testing.T) {
		controller := newController()
		offer, err := controller.Offer(context.Background(), domain.AdmissionRequest{Job: job, Claim: claim})
		assert.NoError(t, "Offer", err)
		lease, err := controller.Commit(context.Background(), offer.OfferID, offer.Fence)
		assert.NoError(t, "Commit", err)
		assert.NoError(t, "Preempt", controller.Preempt(context.Background(), lease.ID, "conformance"))
	})
}
