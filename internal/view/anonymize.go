package view

import (
	"fmt"
	"strings"
)

// anonymizer replaces every free-form string in a frame with a generic label
// that stays the same for the life of the view. Labels are assigned in order
// of first appearance, so they carry no information about the real names
// (unlike a hash, which a list of likely host names could reverse). Values
// that are not labels come from fixed vocabularies; anything else is dropped.
type anonymizer struct {
	machines, projects, jobs, names, tiers map[string]string
}

func newAnonymizer() *anonymizer {
	return &anonymizer{machines: map[string]string{}, projects: map[string]string{}, jobs: map[string]string{}, names: map[string]string{}, tiers: map[string]string{}}
}

var (
	knownStates    = vocabulary("queued", "running", "stopping", "paused", "succeeded", "failed", "cancelled")
	knownPlatforms = vocabulary("cuda", "metal", "xpu", "rocm", "cpu", "fake")
)

func vocabulary(words ...string) map[string]bool {
	set := map[string]bool{}
	for _, w := range words {
		set[w] = true
	}
	return set
}

func label(m map[string]string, real string, format func(int) string) string {
	if real == "" {
		return ""
	}
	if v, ok := m[real]; ok {
		return v
	}
	v := format(len(m) + 1)
	m[real] = v
	return v
}

func letters(n int) string { // 1 → a, 26 → z, 27 → aa
	s := ""
	for n > 0 {
		n--
		s = string(rune('a'+n%26)) + s
		n /= 26
	}
	return s
}

func (a *anonymizer) machine(id string) string {
	return label(a.machines, id, func(n int) string { return fmt.Sprintf("hypha-%d", n) })
}

func (a *anonymizer) frame(f *Frame) {
	f.Anonymized = true
	for i := range f.Machines {
		m := &f.Machines[i]
		m.ID = a.machine(m.ID)
		for k := range m.Devices {
			d := &m.Devices[k]
			d.Platform = known(knownPlatforms, d.Platform)
			d.ID = fmt.Sprintf("%s:%d", or(d.Platform, "gpu"), k)
		}
	}
	for i := range f.Jobs {
		j := &f.Jobs[i]
		j.Project = label(a.projects, j.Project, func(n int) string { return "project-" + letters(n) })
		j.ID = label(a.jobs, j.ID, func(n int) string { return fmt.Sprintf("j%d", n) })
		// The same real name gets the same label; names are free text, so the
		// label never echoes any part of the original.
		j.Name = label(a.names, j.Name, func(n int) string { return fmt.Sprintf("job-%02d", n) })
		j.Machine = a.machine(j.Machine)
		j.Tier = label(a.tiers, j.Tier, func(n int) string {
			return fmt.Sprintf("tier-%d", n)
		})
		if j.Production {
			j.Tier = "production"
		}
		j.State = known(knownStates, j.State)
		j.Needs.Platform = known(knownPlatforms, j.Needs.Platform)
	}
}

func known(set map[string]bool, v string) string {
	if v == "" || set[strings.ToLower(v)] {
		return strings.ToLower(v)
	}
	return "other"
}

func or(v, fallback string) string {
	if v == "" || v == "other" {
		return fallback
	}
	return v
}
