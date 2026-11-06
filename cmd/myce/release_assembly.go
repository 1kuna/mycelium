package main

import (
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"mycelium/internal/myce"
)

// The digest must come from the trusted native assembly step, independently of
// the transported receipt. A self-authenticating receipt cannot prove native checks.
func verifiedAssembly(dir, version, commit, receiptPath, receiptHash string) (myce.ReleaseManifest, error) {
	var m myce.ReleaseManifest
	refuse := func() (myce.ReleaseManifest, error) {
		return m, errors.New("verified assembly receipt, source or artifact mismatch")
	}
	if !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(commit) || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(receiptHash) {
		return refuse()
	}
	// Authenticate and parse one snapshot; reopening after hashing permits a
	// replaced file (or FIFO) to supply unauthenticated native proof.
	data, err := os.ReadFile(receiptPath)
	if err != nil {
		return m, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != receiptHash {
		return refuse()
	}
	type record struct {
		File   string `json:"file"`
		SHA256 string `json:"sha256"`
		Size   int64  `json:"size"`
	}
	type build struct {
		Version   string   `json:"version"`
		Commit    string   `json:"commit"`
		Artifacts []record `json:"artifacts"`
	}
	var receipt struct {
		Version        string           `json:"version"`
		Commit         string           `json:"commit"`
		Builds         map[string]build `json:"builds"`
		ManifestSHA256 string           `json:"manifest_sha256"`
		Darwin         struct {
			Info                myce.VersionInfo `json:"info"`
			DeveloperIDVerified bool             `json:"developer_id_verified"`
		} `json:"darwin"`
	}
	if json.Unmarshal(data, &receipt) != nil || receipt.Version != version || receipt.Commit != commit {
		return refuse()
	}
	manifestPath := filepath.Join(dir, "manifest.unsigned.json")
	data, err = os.ReadFile(manifestPath)
	if err != nil {
		return m, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != receipt.ManifestSHA256 {
		return refuse()
	}
	if json.Unmarshal(data, &m) != nil {
		return refuse()
	}
	if err = m.Validate(); err != nil {
		return m, err
	}
	if m.Version != version || !receipt.Darwin.DeveloperIDVerified || receipt.Darwin.Info != (myce.VersionInfo{Version: version, Commit: commit, OS: "darwin", Arch: "arm64", StartGate: m.StartGate, AttemptProtocol: m.AttemptProtocol, NativeSensing: true}) {
		return refuse()
	}
	expectedBuilds := map[string]int{}
	for _, a := range m.Artifacts {
		expectedBuilds[a.OS]++
	}
	if len(receipt.Builds) != len(expectedBuilds) {
		return refuse()
	}
	for osname, count := range expectedBuilds {
		b, ok := receipt.Builds[osname]
		if !ok || b.Version != version || b.Commit != commit || len(b.Artifacts) != count {
			return refuse()
		}
	}
	for _, a := range m.Artifacts {
		path := filepath.Join(dir, a.File)
		st, err := os.Lstat(path)
		if err != nil {
			return m, err
		}
		if !st.Mode().IsRegular() || st.Size() != a.Size {
			return refuse()
		}
		digest, err := myce.FileSHA256(path)
		if err != nil {
			return m, err
		}
		if digest != a.SHA256 {
			return refuse()
		}
		matches := 0
		for _, r := range receipt.Builds[a.OS].Artifacts {
			if r == (record{a.File, a.SHA256, a.Size}) {
				matches++
			}
		}
		if matches != 1 {
			return refuse()
		}
		// Read metadata only: no candidate executable is run on the publisher.
		info, err := buildinfo.ReadFile(path)
		if err != nil {
			return m, err
		}
		settings := map[string]string{}
		for _, s := range info.Settings {
			settings[s.Key] = s.Value
		}
		cgo := "0"
		if a.OS == "darwin" {
			cgo = "1"
		}
		if settings["GOOS"] != a.OS || settings["GOARCH"] != a.Arch || settings["CGO_ENABLED"] != cgo {
			return refuse()
		}
		// Native assembly verified the build ID's exact version and commit
		// before retaining this artifact's hash/size in its build receipt.
		// The independently pinned assembly digest authenticates that proof;
		// matching the bytes above preserves it without a publisher toolchain.
	}
	return m, nil
}
