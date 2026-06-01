package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type requireFlags []string

func (r *requireFlags) String() string {
	return strings.Join(*r, ",")
}

func (r *requireFlags) Set(value string) error {
	*r = append(*r, value)
	return nil
}

type counters struct {
	total   int
	covered int
}

func main() {
	os.Exit(mainExit(os.Args[1:], os.Stderr))
}

func mainExit(args []string, stderr io.Writer) int {
	if err := runCLI(args); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func runCLI(args []string) error {
	var profile string
	var min float64
	var packageMin float64
	var requires requireFlags
	var fileRequires requireFlags
	var packagePrefixes requireFlags
	var packageExcludes requireFlags
	fs := flag.NewFlagSet("covergate", flag.ContinueOnError)
	fs.StringVar(&profile, "profile", "", "coverage profile path")
	fs.Float64Var(&min, "min", 0, "minimum total line coverage, as 0.85 or 85")
	fs.Float64Var(&packageMin, "package-min", 0, "minimum coverage for matching packages, as 0.85 or 85")
	fs.Var(&packagePrefixes, "package-prefix", "package prefix included in -package-min checks; may repeat")
	fs.Var(&packageExcludes, "package-exclude", "package prefix excluded from -package-min checks; may repeat")
	fs.Var(&requires, "require", "package=minimum coverage requirement; may repeat")
	fs.Var(&fileRequires, "require-file", "file=minimum coverage requirement; may repeat")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return run(profile, gateConfig{
		TotalMin:        normalizeThreshold(min),
		PackageMin:      normalizeThreshold(packageMin),
		PackagePrefixes: normalizePrefixes(packagePrefixes),
		PackageExcludes: normalizePrefixes(packageExcludes),
		Requires:        requires,
		FileRequires:    fileRequires,
	})
}

type gateConfig struct {
	TotalMin        float64
	PackageMin      float64
	PackagePrefixes []string
	PackageExcludes []string
	Requires        []string
	FileRequires    []string
}

func run(profile string, cfg gateConfig) error {
	if profile == "" {
		return fmt.Errorf("-profile is required")
	}
	total, byPackage, byFile, err := readProfile(profile)
	if err != nil {
		return err
	}
	if total.total == 0 {
		return fmt.Errorf("coverage profile has no statements")
	}
	totalPct := total.percent()
	if totalPct+1e-9 < cfg.TotalMin {
		return fmt.Errorf("total coverage %.1f%% is below %.1f%%", totalPct*100, cfg.TotalMin*100)
	}
	if cfg.PackageMin > 0 {
		for pkg, count := range byPackage {
			if !packageSelected(pkg, cfg.PackagePrefixes, cfg.PackageExcludes) {
				continue
			}
			if count.percent()+1e-9 < cfg.PackageMin {
				return fmt.Errorf("%s coverage %.1f%% is below package minimum %.1f%%", pkg, count.percent()*100, cfg.PackageMin*100)
			}
		}
	}
	for _, raw := range cfg.Requires {
		pkg, threshold, err := parseRequirement(raw)
		if err != nil {
			return err
		}
		count, ok := byPackage[pkg]
		if !ok {
			return fmt.Errorf("required package %q is missing from coverage profile", pkg)
		}
		if count.percent()+1e-9 < threshold {
			return fmt.Errorf("%s coverage %.1f%% is below %.1f%%", pkg, count.percent()*100, threshold*100)
		}
	}
	for _, raw := range cfg.FileRequires {
		file, threshold, err := parseRequirement(raw)
		if err != nil {
			return err
		}
		count, ok := byFile[file]
		if !ok {
			return fmt.Errorf("required file %q is missing from coverage profile", file)
		}
		if count.percent()+1e-9 < threshold {
			return fmt.Errorf("%s coverage %.1f%% is below %.1f%%", file, count.percent()*100, threshold*100)
		}
	}
	fmt.Printf("coverage ok: total %.1f%%\n", totalPct*100)
	return nil
}

func packageSelected(pkg string, prefixes, excludes []string) bool {
	if len(prefixes) == 0 {
		return false
	}
	for _, exclude := range excludes {
		if strings.HasPrefix(pkg, exclude) {
			return false
		}
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(pkg, prefix) {
			return true
		}
	}
	return false
}

func readProfile(path string) (counters, map[string]counters, map[string]counters, error) {
	file, err := os.Open(path)
	if err != nil {
		return counters{}, nil, nil, err
	}
	defer file.Close()
	total := counters{}
	byPackage := map[string]counters{}
	byFile := map[string]counters{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return counters{}, nil, nil, fmt.Errorf("invalid coverage line %q", line)
		}
		statements, err := strconv.Atoi(fields[1])
		if err != nil {
			return counters{}, nil, nil, fmt.Errorf("invalid statement count in %q: %w", line, err)
		}
		hits, err := strconv.Atoi(fields[2])
		if err != nil {
			return counters{}, nil, nil, fmt.Errorf("invalid hit count in %q: %w", line, err)
		}
		file := filePath(fields[0])
		pkg := packagePath(fields[0])
		count := byPackage[pkg]
		fileCount := byFile[file]
		count.total += statements
		fileCount.total += statements
		total.total += statements
		if hits > 0 {
			count.covered += statements
			fileCount.covered += statements
			total.covered += statements
		}
		byPackage[pkg] = count
		byFile[file] = fileCount
	}
	if err := scanner.Err(); err != nil {
		return counters{}, nil, nil, err
	}
	return total, byPackage, byFile, nil
}

func filePath(location string) string {
	file := location
	if before, _, ok := strings.Cut(location, ":"); ok {
		file = before
	}
	return strings.TrimPrefix(file, "mycelium/")
}

func packagePath(location string) string {
	file := filePath(location)
	dir := file
	if idx := strings.LastIndex(dir, "/"); idx >= 0 {
		dir = dir[:idx]
	}
	return strings.TrimPrefix(dir, "mycelium/")
}

func parseRequirement(raw string) (string, float64, error) {
	pkg, value, ok := strings.Cut(raw, "=")
	if !ok || pkg == "" || value == "" {
		return "", 0, fmt.Errorf("invalid -require %q, want package=threshold", raw)
	}
	threshold, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return "", 0, err
	}
	return strings.TrimPrefix(pkg, "mycelium/"), normalizeThreshold(threshold), nil
}

func normalizePrefixes(prefixes []string) []string {
	out := make([]string, len(prefixes))
	for i, prefix := range prefixes {
		out[i] = strings.TrimPrefix(prefix, "mycelium/")
	}
	return out
}

func normalizeThreshold(value float64) float64 {
	if value > 1 {
		return value / 100
	}
	return value
}

func (c counters) percent() float64 {
	if c.total == 0 {
		return 0
	}
	return float64(c.covered) / float64(c.total)
}
