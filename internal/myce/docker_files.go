package myce

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const dockerFilesCapability = "docker_files_v1"

var errDockerFileImageVolumes = errors.New("Docker file jobs require images without declared VOLUME mounts")

func dockerFileJob(s Spec) bool {
	return s.Run.Docker && len(s.Files)+len(s.Outputs) > 0
}

// File jobs have only worker-owned mounts, identity and environment. Parse the
// direct argv first so image/payload arguments are never mistaken for options.
func validateDockerFiles(s Spec) error {
	if !dockerFileJob(s) {
		return nil
	}
	if len(s.Inputs) > 0 || s.Run.Workdir != "" || s.PrepareSource || s.extensionJob() || s.Execution != nil || s.Environment.Name != "" || s.Environment.LockFile != "" || s.Environment.Python != "" || len(s.Environment.Platforms) > 0 {
		return errors.New("Docker files require a plain batch without NAS inputs, workdir, prepared source, execution or environment")
	}
	commands := [][]string{s.Run.Command}
	if len(s.Run.Env) > 0 {
		return errors.New("Docker files use only worker-owned environment; run.env is unsupported")
	}
	for _, sidecar := range s.Run.DockerSidecars {
		if len(sidecar.Env) > 0 {
			return errors.New("Docker files use only worker-owned environment; sidecar env is unsupported")
		}
		commands = append(commands, sidecar.Command)
	}
	for _, command := range commands {
		if _, err := dockerFileImage(command); err != nil {
			return err
		}
	}
	return nil
}

func dockerFileImage(command []string) (string, error) {
	args, err := dockerRunArgs(command)
	if err != nil {
		return "", err
	}
	i := 0
	for ; i < len(args) && strings.HasPrefix(args[i], "-"); i++ {
		flag, _, equals := strings.Cut(args[i], "=")
		switch flag {
		case "--volume", "-v", "--env", "-e", "--workdir", "-w", "--user", "-u", "--group-add":
			return "", fmt.Errorf("Docker files reserve mounts, environment and user/workdir; %s is unsupported", flag)
		}
		if flag != "--init" && !equals {
			i++
		}
	}
	return args[i], nil // dockerRunArgs already proved that the image exists in argv.
}

func (d *dockerAttempt) checkFileImages(j Job) error {
	commands := [][]string{j.Spec.Run.Command}
	for _, sidecar := range j.Spec.Run.DockerSidecars {
		commands = append(commands, sidecar.Command)
	}
	for _, command := range commands {
		image, err := dockerFileImage(command)
		if err != nil {
			return err
		}
		out, err := d.call("image", "inspect", "--format", "{{json .Config.Volumes}}", image)
		if err != nil {
			return d.recordInfrastructure(infrastructureFact{Kind: launchFailed, At: d.clock.Now().UTC()}, err)
		}
		var volumes map[string]json.RawMessage
		if err := json.Unmarshal(out, &volumes); err != nil {
			return errors.New("Docker file image volume metadata unavailable")
		}
		if len(volumes) > 0 {
			return errDockerFileImageVolumes
		}
	}
	return nil
}

// The mount plan is persisted with Docker custody and checked before start and
// throughout observation. Image-declared volumes are refused as well.
type dockerFileMount struct {
	Type        string
	Source      string
	Destination string
	RW          bool
}

func dockerFileMounts(j Job, stateDir, checkpoint string) ([]dockerFileMount, error) {
	root := filepath.Join(stateDir, "job-files", j.ID, strconv.Itoa(j.Attempt))
	if j.Spec.Run.Workdir != filepath.Join(root, "work") {
		return nil, errors.New("Docker files require the worker-prepared attempt workdir")
	}
	mounts := []dockerFileMount{
		{Type: "bind", Source: filepath.Join(root, "inputs"), Destination: "/myce/inputs"},
		{Type: "bind", Source: j.Spec.Run.Workdir, Destination: "/myce/work", RW: true},
		{Type: "bind", Source: checkpoint, Destination: "/myce/checkpoint", RW: true},
		{Type: "bind", Source: filepath.Dir(applicationProgressFile(stateDir, j)), Destination: "/myce/progress", RW: true},
	}
	for i := range mounts {
		path := mounts[i].Source
		st, err := os.Lstat(path)
		if err != nil {
			return nil, err
		}
		if !filepath.IsAbs(path) || !st.IsDir() || strings.ContainsAny(path, ",\n\r") {
			return nil, errors.New("Docker mount requires a private attempt directory and a path without commas or newlines")
		}
		mounts[i].Source, err = filepath.EvalSymlinks(path)
		if err != nil {
			return nil, err
		}
	}
	return mounts, nil
}

func (d *dockerAttempt) fileMountViolation(c dockerInspection) string {
	if len(d.state.FileMounts) == 0 {
		return ""
	}
	if len(c.Mounts) != len(d.state.FileMounts) {
		return "Docker file mounts missing or unexpected; daemon must access worker-local attempt directories"
	}
	for _, want := range d.state.FileMounts {
		found := false
		for _, got := range c.Mounts {
			if got == want {
				found = true
			}
		}
		if !found {
			return "Docker file mount source, destination or read-only mode not enforced"
		}
	}
	return ""
}
