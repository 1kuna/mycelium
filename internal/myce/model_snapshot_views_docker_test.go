//go:build linux || darwin

package myce

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Real local daemon only, with an already present image; never pull or change
// daemon configuration. The ordinary suite still checks the worker's create
// request in TestModelSnapshotsDockerMountMatchesNativePath.
func TestModelSnapshotDockerReadOnlyViews(t *testing.T) {
	t.Setenv("PATH", dockerTestOriginalPath)
	binary, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("Docker CLI unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	endpoint := defaultDockerEndpoint(os.Getenv("DOCKER_HOST"))
	call := func(args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, binary, append([]string{"--host", endpoint}, args...)...)
		cmd.Env = dockerClientEnvironment()
		return cmd.Output()
	}
	if data, err := call("info", "--format", "{{.OSType}}"); err != nil || strings.TrimSpace(string(data)) != "linux" {
		t.Skip("local Linux Docker daemon unavailable")
	}
	image := os.Getenv("MYCE_REAL_DOCKER_IMAGE")
	if image == "" {
		data, err := call("image", "inspect", "busybox:1.37.0", "--format", "{{json .RepoDigests}}")
		var digests []string
		if err != nil || json.Unmarshal(data, &digests) != nil || len(digests) == 0 {
			t.Skip("local digest-pinned busybox:1.37.0 unavailable")
		}
		image = digests[0]
	}
	modelSnapshotViews(t, "docker", func(t *testing.T, w Worker, cmd *exec.Cmd, spool, work string, env map[string]string) {
		// Use the same managed model mount description as the worker. The existing
		// create-request regression verifies that each becomes a readonly bind.
		mounts := dockerModelMounts(snapshotJob("org/tiny"), env)
		if len(mounts) != 1 {
			t.Fatal("missing managed model mount")
		}
		mount := mounts[0]
		var id [8]byte
		if _, err := rand.Read(id[:]); err != nil {
			t.Fatal(err)
		}
		name := fmt.Sprintf("myce-model-view-%x", id)
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(cleanup, binary, "--host", endpoint, "rm", "--force", name)
			cmd.Env = dockerClientEnvironment()
			if data, err := cmd.CombinedOutput(); err != nil && !strings.Contains(string(data), "No such container") {
				t.Errorf("owned model-view container cleanup: %v: %s", err, data)
			}
		})
		args := []string{binary, "--host", endpoint, "run", "--name", name, "--rm", "-i", "--pull=never", "--network=none", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges", "--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), "--mount", "type=bind,src=" + mount.Source + ",dst=" + mount.Destination + ",readonly", "--env", "MYCE_MODEL_BASE=" + mount.Destination, image, "sh", "-ec", cmd.Args[2]}
		cmd.Path = binary
		cmd.Args = append([]string{binary, "--host", endpoint}, gateDockerArgs(t, endpoint, args[3:])...)
		cmd.Env = dockerClientEnvironment()
	})
}
