package myce

import (
	"testing"
	"time"
)

func TestExecutionMemoryEnvelopeOptionalMembers(t *testing.T) {
	q, clock := fixture(t)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 4
	s.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"sleep", "60"}}}
	if _, err := q.Submit(s); err != nil {
		t.Fatal("envelope-only native group refused:", err)
	}
	clock.Advance(time.Second)
	s.Run = RunSpec{Docker: true, DockerNetwork: "private", Command: []string{"docker", "run", "image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, DockerSidecars: []DockerSidecar{{Name: "helper", Command: []string{"docker", "run", "image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}}
	if err := s.validateDockerGroup(); err != nil {
		t.Fatal("envelope-only Docker group refused:", err)
	}
}
