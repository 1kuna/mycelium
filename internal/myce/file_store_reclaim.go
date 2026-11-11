package myce

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

// Refusals that collecting already-authorized files can relieve: store space,
// and a project quota counted from retained (unexpired) references.
func fileStoreSpaceRefusal(err error) bool {
	var he *HTTPError
	return errors.As(err, &he) && (he.Code == "file_store_capacity" || he.Code == "project_file_store_quota")
}

// Called with no queue or lifecycle locks, after a write was refused for space.
// It expires records whose retention deadline has already passed and collects
// bytes that retention or an owner release already authorized, until the
// writer's need is covered. It never widens what may be deleted: ordinary
// maintenance would do the same later, but yields to control traffic while a
// writer waits on a refusal. Reports whether anything was expired or freed.
func (q *Queue) reclaimForAdmission(ctx context.Context, need uint64) bool {
	if need == 0 {
		need = 1
	}
	var expired uint64
	if err := q.updateRequest(ctx, func() error { expired = q.expireDueFiles(need); return nil }, nil); err != nil {
		log.Printf("file retention deferred: %v", err)
	}
	freed, err := q.reclaimPublishedFilesUntil(ctx, need)
	if err != nil {
		log.Printf("file retention deferred: %v", err)
	}
	// A quota refusal can clear on expiry alone.
	return freed > 0 || expired > 0
}

// Bytes kept by finished jobs whose files have no passed expiry deadline. They
// stay until an owner release or a retention policy expires them. Shared bytes
// still referenced elsewhere are counted, so this is an upper bound on what a
// release would free.
type finishedFileHolders struct {
	project                          string
	bytes, projectBytes              uint64
	projectJobs                      int
	queueRetention, projectRetention time.Duration
}

// Caller holds q.mu; manifest metadata only, no store I/O.
func (q *Queue) finishedFileHolders(project string) finishedFileHolders {
	h := finishedFileHolders{project: project, queueRetention: q.fileRetention}
	h.projectRetention, _ = parseProjectFileRetention(q.s.Policies[project].FileRetention)
	seen, projectSeen := map[string]bool{}, map[string]bool{}
	count := func(j *Job) {
		if j == nil || !terminal(j.State) || j.FilesExpired || !hasJobFiles(j) {
			return
		}
		mine := project != "" && j.Spec.Project == project
		if mine {
			h.projectJobs++
		}
		for _, files := range jobFileManifests(j) {
			for _, ref := range files {
				if !seen[ref.SHA256] {
					seen[ref.SHA256] = true
					h.bytes = addDiskBytes(h.bytes, uint64(ref.SizeBytes))
				}
				if mine && !projectSeen[ref.SHA256] {
					projectSeen[ref.SHA256] = true
					h.projectBytes = addDiskBytes(h.projectBytes, uint64(ref.SizeBytes))
				}
			}
		}
	}
	for _, j := range q.s.Jobs {
		count(j)
	}
	for _, j := range q.retiredSubmissionJobs("", nil) {
		count(j)
	}
	return h
}

func retentionText(d time.Duration) string {
	if d == 0 {
		return "none"
	}
	return d.String()
}

// The cause and next step for a refused writer, in one sentence.
func (h finishedFileHolders) advice() string {
	if h.bytes == 0 {
		return "; finished jobs hold no releasable files; free space on the queue file-store volume or increase --file-store-bytes"
	}
	who := "project"
	if h.project != "" {
		who = fmt.Sprintf("project %s", h.project)
	}
	msg := fmt.Sprintf("; finished jobs hold %d bytes until released (queue file retention %s", h.bytes, retentionText(h.queueRetention))
	if h.project != "" {
		msg += fmt.Sprintf(", %s file_retention %s; %s holds %d bytes in %d finished jobs", who, retentionText(h.projectRetention), who, h.projectBytes, h.projectJobs)
	}
	project := h.project
	if project == "" {
		project = "PROJECT"
	}
	return msg + fmt.Sprintf("). Next step: release finished work with `myce file release plan --project %s JOB_ID...` then `myce file release apply --accept-expiry PLAN`, or set file_retention in the project policy; an operator can also free space on the queue volume or increase --file-store-bytes", project)
}

// Adds the retained-file cause and next step to a store-capacity refusal.
func (q *Queue) explainFileStoreRefusal(ctx context.Context, err error) error {
	var he *HTTPError
	if !errors.As(err, &he) || he.Code != "file_store_capacity" {
		return err
	}
	project := ""
	if s, ok := ctx.Value(fileStoreSubmissionKey{}).(Spec); ok {
		project = s.Project
	}
	if lockErr := lockUploadContext(ctx, q.mu.RLock, q.mu.RUnlock); lockErr != nil {
		return err
	}
	holders := q.finishedFileHolders(project)
	q.mu.RUnlock()
	explained := *he
	explained.Message += holders.advice()
	return &explained
}

// Names which bound set free space: the configured allowance or the volume.
func (s FileStoreStatus) capacityBound() string {
	if s.diskTotal == 0 {
		return ""
	}
	if s.allowanceFree <= s.volumeFree {
		return fmt.Sprintf("; bound=store_allowance (--file-store-bytes=%d)", s.LimitBytes)
	}
	other := s.diskTotal - min(s.diskTotal, addDiskBytes(s.DiskFreeBytes, s.UsedBytes))
	return fmt.Sprintf("; bound=volume (other data on the queue volume=%d bytes)", other)
}
