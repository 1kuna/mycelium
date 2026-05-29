package main

import (
	"bufio"
	"flag"
	"fmt"
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
	var profile string
	var min float64
	var requires requireFlags
	flag.StringVar(&profile, "profile", "", "coverage profile path")
	flag.Float64Var(&min, "min", 0, "minimum total line coverage, as 0.85 or 85")
	flag.Var(&requires, "require", "package=minimum coverage requirement; may repeat")
	flag.Parse()
	if err := run(profile, normalizeThreshold(min), requires); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(profile string, min float64, requires []string) error {
	if profile == "" {
		return fmt.Errorf("-profile is required")
	}
	total, byPackage, err := readProfile(profile)
	if err != nil {
		return err
	}
	if total.total == 0 {
		return fmt.Errorf("coverage profile has no statements")
	}
	totalPct := total.percent()
	if totalPct+1e-9 < min {
		return fmt.Errorf("total coverage %.1f%% is below %.1f%%", totalPct*100, min*100)
	}
	for _, raw := range requires {
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
	fmt.Printf("coverage ok: total %.1f%%\n", totalPct*100)
	return nil
}

func readProfile(path string) (counters, map[string]counters, error) {
	file, err := os.Open(path)
	if err != nil {
		return counters{}, nil, err
	}
	defer file.Close()
	total := counters{}
	byPackage := map[string]counters{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return counters{}, nil, fmt.Errorf("invalid coverage line %q", line)
		}
		statements, err := strconv.Atoi(fields[1])
		if err != nil {
			return counters{}, nil, fmt.Errorf("invalid statement count in %q: %w", line, err)
		}
		hits, err := strconv.Atoi(fields[2])
		if err != nil {
			return counters{}, nil, fmt.Errorf("invalid hit count in %q: %w", line, err)
		}
		pkg := packagePath(fields[0])
		count := byPackage[pkg]
		count.total += statements
		total.total += statements
		if hits > 0 {
			count.covered += statements
			total.covered += statements
		}
		byPackage[pkg] = count
	}
	if err := scanner.Err(); err != nil {
		return counters{}, nil, err
	}
	return total, byPackage, nil
}

func packagePath(location string) string {
	file := location
	if before, _, ok := strings.Cut(location, ":"); ok {
		file = before
	}
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
