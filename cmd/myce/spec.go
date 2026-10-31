package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

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
	return s, nil
}

func specCommand(args []string, out, errOut io.Writer) error {
	const usage = "usage: myce spec check FILE"
	const scope = "Checks static validity only (YAML or JSON); does not check live eligibility, admission or qualification."
	help := func() {
		fmt.Fprintln(errOut, usage)
		fmt.Fprintln(errOut, scope)
		fmt.Fprintln(errOut, "Offline: no fleet authority, network requests, uploads or fleet changes.")
		fmt.Fprintln(errOut, "Exit codes: 0 valid (or help); 1 invalid or unreadable spec; 2 usage error.")
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
	if err := f.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return waitError(2, err.Error())
	}
	if f.NArg() != 1 {
		return waitError(2, usage)
	}
	path := f.Arg(0)
	s, err := readJobSpec(path)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if err := s.Validate(); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	_, err = fmt.Fprintf(out, "%s: valid (static checks only)\n", path)
	return err
}
