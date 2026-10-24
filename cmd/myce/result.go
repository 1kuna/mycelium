package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"mycelium/internal/myce"
)

// ResultError gives shell consumers a stable failure code while preserving the cause.
type ResultError struct {
	Code  string
	Err   error
	token string
}

func (e *ResultError) Error() string {
	if e.token == "" {
		return e.Code + ": " + e.Err.Error()
	}
	return strings.ReplaceAll(e.Code+": "+e.Err.Error(), e.token, "[redacted]")
}
func (e *ResultError) Unwrap() error { return e.Err }

func resultCommand(ctx context.Context, args []string, errOut io.Writer) error {
	f := flag.NewFlagSet("result", flag.ContinueOnError)
	f.SetOutput(errOut)
	authority := installedClientFlags(f)
	out := f.String("out", "", "directory for verified accepted artifacts")
	// Support the documented job-id followed by flags, as with logs.
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		args = append(append([]string{}, args[1:]...), args[0])
	}
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 1 || *out == "" {
		return errors.New("usage: myce result <job-id> --out DIR")
	}
	c, err := authority.client()
	if err != nil {
		return err
	}
	if c.HTTP != nil {
		defer c.HTTP.CloseIdleConnections()
	}
	failure := func(err error) error {
		code := "result_download_failed"
		var api *myce.HTTPError
		if errors.As(err, &api) {
			code = api.Code
			if api.Status == 410 {
				code = "result_expired"
			}
			if code == "" {
				code = "result_download_failed"
			}
		}
		return &ResultError{Code: code, Err: err, token: c.Token}
	}
	j, err := c.GetJob(ctx, f.Arg(0))
	if err != nil {
		return failure(err)
	}
	if j.State != "succeeded" || len(j.OutputRefs) == 0 {
		return &ResultError{Code: "result_not_accepted", Err: errors.New("job has no accepted result")}
	}
	// Validate every manifest key before using it as a local filename.
	for name := range j.OutputRefs {
		if name == "." || !fs.ValidPath(name) || strings.ContainsAny(name, "\\:") {
			return &ResultError{Code: "result_invalid", Err: errors.New("result names must be clean relative paths")}
		}
	}
	for name := range j.OutputRefs {
		dest := filepath.Join(*out, filepath.FromSlash(name))
		// Existing parent symlinks must not redirect automatic result placement.
		root, err := filepath.Abs(*out)
		if err != nil {
			return failure(err)
		}
		absolute, err := filepath.Abs(dest)
		if err != nil {
			return failure(err)
		}
		for parent := filepath.Dir(absolute); parent != root; parent = filepath.Dir(parent) {
			info, err := os.Lstat(parent)
			if err != nil && !os.IsNotExist(err) {
				return failure(err)
			}
			if err == nil && !info.IsDir() {
				return &ResultError{Code: "result_invalid", Err: errors.New("result parent must be a directory")}
			}
		}
		if err := c.DownloadResult(ctx, j.ID, name, dest); err != nil {
			return failure(err)
		}
	}
	return nil
}
