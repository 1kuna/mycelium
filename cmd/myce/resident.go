package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"mycelium/internal/myce"
)

// This command is a payload helper. It never opens fleet configuration.
func residentCommand(args []string, in io.Reader, out io.Writer) error {
	f := flag.NewFlagSet("resident", flag.ContinueOnError)
	method := f.String("method", "POST", "HTTP method")
	endpoint := f.String("path", "/", "engine endpoint, including optional query")
	input := f.String("input", "", "request body file (default stdin)")
	content := f.String("content-type", "application/octet-stream", "request content type")
	if err := f.Parse(args); err != nil {
		return err
	}
	u, err := url.ParseRequestURI(*endpoint)
	if err != nil || !strings.HasPrefix(*endpoint, "/") || strings.HasPrefix(*endpoint, "//") || u.IsAbs() {
		return errors.New("resident path must be a local engine endpoint")
	}
	path, key := os.Getenv("MYCE_RESIDENT_PIPE"), os.Getenv("MYCE_RESIDENT_KEY")
	if path == "" || key == "" {
		return errors.New("resident requires an opted-in running stage")
	}
	if *input != "" {
		file, err := os.Open(*input)
		if err != nil {
			return err
		}
		defer file.Close()
		in = file
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	req, err := http.NewRequestWithContext(ctx, *method, "http://resident"+*endpoint, in)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", *content)
	response, err := myce.ResidentRequest(ctx, path, key, req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("resident engine returned HTTP %d", response.StatusCode)
	}
	_, err = io.Copy(out, response.Body)
	return err
}
