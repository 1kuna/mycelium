package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
)

// readJobSpec is the strict author-file decoder shared by submit and spec check.
func readJobSpec(path string) (myce.Spec, error) {
	var s myce.Spec
	file, err := os.Open(path)
	if err != nil {
		return s, err
	}
	defer file.Close()
	d := yaml.NewDecoder(io.LimitReader(file, 1<<20))
	d.KnownFields(true)
	if err := d.Decode(&s); err != nil {
		return s, err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return s, errors.New("expected one YAML job")
	}
	if err := foldAuthoredAdvanced(path, &s); err != nil {
		return s, err
	}
	s.AuthoringOrigin = myce.OriginJobSpec
	return s, nil
}

var advancedNoted sync.Map

// foldAuthoredAdvanced applies the MY-54 advanced section to an authored spec
// and notes deprecated top-level placement on stderr, once per source.
// Top-level placement is accepted for one release.
func foldAuthoredAdvanced(source string, s *myce.Spec) error {
	deprecated, err := s.FoldAdvanced()
	if err != nil {
		return err
	}
	if len(deprecated) > 0 {
		if _, seen := advancedNoted.LoadOrStore(source, true); !seen {
			fmt.Fprintf(os.Stderr, "myce: %s: top-level %s is deprecated; move it under advanced: (accepted for one release, MY-54)\n", source, strings.Join(deprecated, ", "))
		}
	}
	return nil
}

func specCommand(args []string, out, errOut io.Writer) error {
	const usage = "usage: myce spec check [--resolve [--files-from DIR] [--inspect-nas] [--entry NAME] [--platform P] [--json]] FILE|PROFILE"
	const scope = "Checks static validity only (YAML or JSON); does not check live eligibility, admission or qualification."
	help := func() {
		fmt.Fprintln(errOut, usage)
		fmt.Fprintln(errOut, scope)
		fmt.Fprintln(errOut, "Offline: no fleet authority, network requests, uploads or fleet changes.")
		fmt.Fprintln(errOut, "--resolve (job spec, profile or adapter): asks the queue, read-only, which targets can run it and names each missing input or environment with its side and fix. Nothing is submitted, uploaded, built or reserved; submit revalidates.")
		fmt.Fprintln(errOut, "Exit codes: 0 valid (or help; with --resolve, at least one eligible target); 1 invalid, unreadable or no eligible target; 2 usage error.")
	}
	if len(args) == 1 && (args[0] == "--help" || args[0] == "-h") {
		help()
		return nil
	}
	if len(args) == 0 || args[0] != "check" {
		return waitError(2, usage)
	}
	f := flag.NewFlagSet("spec check", flag.ContinueOnError)
	f.SetOutput(errOut)
	f.Usage = help
	resolve := f.Bool("resolve", false, "ask the queue which targets can run this job (read-only)")
	inspectNAS := f.Bool("inspect-nas", false, "request cached worker NAS existence checks; repeat after a heartbeat for results")
	filesFrom := f.String("files-from", "", "local folder the job's inputs are submitted from")
	entry := f.String("entry", "", "adapter entry (default: adapter default)")
	platform := f.String("platform", "", "narrow an adapter entry to one allowed platform (requires --resolve)")
	asJSON := f.Bool("json", false, "structured resolve report")
	authority := installedClientFlags(f)
	if err := f.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return waitError(2, err.Error())
	}
	var narrowing []string
	f.Visit(func(v *flag.Flag) {
		if v.Name == "platform" {
			narrowing = []string{*platform}
		}
	})
	if f.NArg() != 1 || !*resolve && (*entry != "" || len(narrowing) > 0 || *asJSON || *filesFrom != "" || *inspectNAS) {
		return waitError(2, usage)
	}
	path := f.Arg(0)
	request, findings, err := resolveInput(path, *filesFrom, *entry, *resolve, narrowing...)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	request.InspectNAS = *inspectNAS
	if request.Recipe == nil {
		if err := request.Spec.Validate(); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	if !*resolve {
		_, err = fmt.Fprintf(out, "%s: valid (static checks only)\n", path)
		return err
	}
	client, err := authority.client()
	if err != nil {
		return err
	}
	var report myce.ResolveReport
	if err := client.Call(context.Background(), "POST", "/api/resolve", request, &report); err != nil {
		var h *myce.HTTPError
		if errors.As(err, &h) && h.Status == 404 {
			return errors.New("this queue predates resolve; upgrade it, or use myce portability")
		}
		return err
	}
	report.Linkage = findings.linkage
	report.Notes = append(report.Notes, findings.notes...)
	if report.Portability != nil {
		report.Portability.Linkage = findings.linkage
	}
	report.Resolved = append(findings.resolved, report.Resolved...)
	report.Findings = append(findings.findings, report.Findings...)
	if h := myce.ResolveFindingHold(report.Findings); h != nil {
		report.Hold = h
	}
	if !*asJSON {
		fmt.Fprintf(out, "%s: valid (static checks); resolving against the queue\n", path)
	}
	if err := myce.WriteResolve(out, report, *asJSON); err != nil {
		return err
	}
	if !slices.ContainsFunc(report.Targets, func(t myce.ResolveTarget) bool { return t.Eligible }) {
		return waitError(1, path+": no eligible target; see findings")
	}
	return nil
}

type submitterFindings struct {
	linkage  []myce.MachOLinkage
	notes    []string
	unstaged []string
	resolved []string
	findings []myce.ResolveFinding
}

func (s *submitterFindings) missing(field, problem, fix string, d error) {
	f := myce.ResolveFinding{Field: field, Side: "submitter", Problem: problem, Fix: fix}
	if d != nil {
		f.Problem = d.Error()
	}
	s.findings = append(s.findings, f)
}

// placeholderRef stands in for bytes resolve never uploads or hashes. Only
// size and a stable fake identity reach the queue; submit computes real ones.
func placeholderRef(name string, size int64) myce.FileRef {
	sum := sha256.Sum256([]byte("resolve-placeholder:" + name))
	return myce.FileRef{SHA256: hex.EncodeToString(sum[:]), SizeBytes: size}
}

// resolveInput reads a job spec, or for --resolve a profile or adapter entry,
// and checks submitter-side paths; declared local shared input hashes are verified.
func resolveInput(path, filesFrom, entry string, live bool, platform ...string) (myce.ResolveRequest, submitterFindings, error) {
	var found submitterFindings
	s, err := readJobSpec(path)
	if err == nil {
		if len(platform) > 0 {
			return myce.ResolveRequest{}, found, errors.New("--platform requires an adapter entry")
		}
		if filesFrom != "" {
			lowerInputs(&s, filesFrom, "files-from", &found)
		} else if live && len(s.Inputs) > 0 && s.Kind == "" && s.Program == nil {
			found.missing("inputs", "plain-command inputs are uploaded from a local folder, but no --files-from was given", "Pass the same --files-from DIR you will give myce submit.", nil)
			lowerInputs(&s, "", "files-from", &found)
		} else if len(s.Inputs) > 0 {
			found.resolved = append(found.resolved, "inputs: NAS-relative; each worker reads them under its own --checkpoint-root (not checked by resolve)")
		}
		return myce.ResolveRequest{Spec: s, UnstagedInputs: found.unstaged}, found, nil
	}
	if !live {
		return myce.ResolveRequest{}, found, err
	}
	specErr := err
	p, root, _, err := readProfile(path)
	if err == nil && len(platform) > 0 {
		return myce.ResolveRequest{}, found, errors.New("--platform requires an adapter entry")
	}
	if err != nil {
		data, readErr := os.ReadFile(path)
		a, adapterErr := decodeAdapter(data)
		if readErr != nil || adapterErr != nil {
			return myce.ResolveRequest{}, found, specErr
		}
		if p, _, err = adapterProfile(a, entry, platform...); err != nil {
			return myce.ResolveRequest{}, found, err
		}
		if root, err = os.Getwd(); err != nil {
			return myce.ResolveRequest{}, found, err
		}
	}
	inspectSourceLinkage(root, p.Source, &found)
	if p.Native != nil {
		sourceUnverified := !bundleNativeMetadata(p.Native, p.Source, root)
		finding, _ := sourceBundleFinding(root, p.Source)
		if finding != nil {
			found.findings = append(found.findings, *finding)
		}
		return myce.ResolveRequest{Spec: p.Spec, Recipe: p.Native, SourceUnverified: sourceUnverified, RequirementReasons: p.RequirementReasons}, found, nil
	}
	lowerInputs(&p.Spec, root, "project-root", &found)
	if err := attachEnvironmentLock(&p.Spec, root); err != nil {
		found.missing("environment.lock_file", "", "Put the lock and its companion file together under the project root, or use an absolute queue-readable path.", err)
	}
	for name, input := range p.SharedInputs {
		if input.Local == "" {
			continue
		}
		found.unstaged = append(found.unstaged, "shared/"+name)
		local := input.Local
		if !filepath.IsAbs(local) {
			local = filepath.Join(root, local)
		}
		info, err := os.Stat(local)
		if err != nil {
			found.missing("shared_inputs."+name+".local", "", "Create the file, or reference an already imported sha256/size or NAS location.", myce.PathReadError("shared_inputs."+name+".local", "submitter", input.Local, "project-root", "", err))
			info = nil
		}
		if info != nil && input.SizeBytes != 0 && input.SizeBytes != info.Size() {
			found.missing("shared_inputs."+name+".size_bytes", "shared input "+name+": size differs", "Correct size_bytes to match the local file.", nil)
		}
		if info != nil && input.SHA256 != "" {
			ref, hashErr := myce.IdentifyFile(local)
			if hashErr != nil {
				found.missing("shared_inputs."+name+".local", "cannot identify shared input "+name, "Make the local file readable.", nil)
			} else if input.SHA256 != ref.SHA256 {
				found.missing("shared_inputs."+name+".sha256", "shared input "+name+": sha256 differs", "Correct sha256 to match the local file.", nil)
			}
		}
		if input.SHA256 == "" {
			size := int64(1)
			if info != nil {
				size = info.Size()
			}
			ref := placeholderRef(name, size)
			input.SHA256, input.SizeBytes = ref.SHA256, ref.SizeBytes
		} else if info != nil && input.SizeBytes == 0 {
			input.SizeBytes = info.Size()
		}
		input.Local = ""
		p.SharedInputs[name] = input
	}
	finding, size := sourceBundleFinding(root, p.Source)
	if finding != nil {
		found.findings = append(found.findings, *finding)
	}
	// Profiles always ship their source as the frozen archive submit attaches.
	if p.Files == nil {
		p.Files = myce.FileManifest{}
	}
	found.unstaged = append(found.unstaged, "files/.myce-source.tar")
	p.Files[".myce-source.tar"] = placeholderRef(".myce-source.tar", max(1, size))
	p.PrepareSource = true
	return myce.ResolveRequest{Spec: p.Spec, RequirementReasons: p.RequirementReasons, UnstagedInputs: found.unstaged}, found, nil
}

// lowerInputs mirrors submit's inputs-to-files lowering with stat instead of
// hashing or uploading. An empty root lowers names without checking files.
func lowerInputs(s *myce.Spec, root, base string, found *submitterFindings) {
	if len(s.Inputs) == 0 {
		return
	}
	files := myce.FileManifest{}
	staged := 0
	for _, name := range slices.Sorted(maps.Keys(s.Inputs)) {
		found.unstaged = append(found.unstaged, "files/"+name)
		source := s.Inputs[name]
		local := source
		if base == "files-from" && !profileRelative(source) {
			found.missing("inputs."+name, "input source must be relative and stay within "+base, "Use a relative input mapping under "+base+".", nil)
			files[name] = placeholderRef(name, 1)
			continue
		}
		if root == "" {
			files[name] = placeholderRef(name, 1)
			continue
		}
		if !filepath.IsAbs(local) {
			local = filepath.Join(root, local)
		}
		info, err := os.Stat(local)
		if err == nil && !info.Mode().IsRegular() {
			err = errors.New("input is not a regular file")
		}
		if err != nil {
			fix := "Create the file under " + base + ", or correct the mapping."
			if os.IsNotExist(err) || os.IsPermission(err) {
				err = myce.PathReadError("inputs."+name, "submitter", source, base, "", err)
			}
			found.missing("inputs."+name, "", fix, err)
			files[name] = placeholderRef(name, 1)
			continue
		}
		files[name] = placeholderRef(name, info.Size())
		staged++
	}
	if staged > 0 {
		found.resolved = append(found.resolved, fmt.Sprintf("inputs: %d file(s) readable by the submitter; submit uploads them through the file store, so any worker can stage them", staged))
	}
	s.Files, s.Inputs = files, nil
}

// largeSourceMember is where repacking stops being noise: a source archive is
// stored whole, so any edit stores every unchanged large member again (#539).
const largeSourceMember = 16 << 20

// sourceBundleFinding warns when large members dominate a source archive. It
// lists the existing inventory (directory walk or Git tree) without hashing.
func sourceBundleFinding(root string, src profileSource) (*myce.ResolveFinding, int64) {
	type member struct {
		name string
		size int64
	}
	var members []member
	var total int64
	add := func(name string, size int64) {
		total += size
		if size >= largeSourceMember {
			members = append(members, member{name, size})
		}
	}
	switch {
	case src.Directory != "":
		_ = walkProfileDirectory(context.Background(), root, src.Directory, func(name, _ string, info os.FileInfo) error {
			add(name, info.Size())
			return nil
		})
	case src.Git != "":
		args := append([]string{"-C", root, "--literal-pathspecs", "ls-tree", "-r", "-l", "-z", "--full-tree", src.Git, "--"}, src.Paths...)
		data, err := exec.Command("git", args...).Output()
		if err != nil {
			return nil, 0
		}
		for _, row := range bytes.Split(data, []byte{0}) {
			header, name, ok := strings.Cut(string(row), "\t")
			fields := strings.Fields(header)
			if !ok || len(fields) != 4 {
				continue
			}
			size, _ := strconv.ParseInt(fields[3], 10, 64)
			add(name, size)
		}
	}
	var large int64
	for _, m := range members {
		large += m.size
	}
	if total == 0 || large*2 < total {
		return nil, total
	}
	slices.SortFunc(members, func(a, b member) int { return int(b.size>>20) - int(a.size>>20) })
	names := []string{}
	for i, m := range members {
		if i == 5 {
			names = append(names, fmt.Sprintf("%d more", len(members)-5))
			break
		}
		names = append(names, fmt.Sprintf("%s (%d MiB)", m.name, m.size>>20))
	}
	return &myce.ResolveFinding{Field: "source", Side: "submitter", Problem: fmt.Sprintf("source is %d MiB and %d%% of it is large members: %s. Each source change repacks them into a new archive, so the store cannot deduplicate them.", total>>20, large*100/total, strings.Join(names, ", ")), Fix: "Move each large unchanging member out of the source into shared_inputs (`myce file put PATH` or a profile `local:` import, then reference its sha256) and read it through $MYCE_INPUT_NAME. A Git source can be narrowed with source.paths; a directory source always archives every regular file in it."}, total
}
