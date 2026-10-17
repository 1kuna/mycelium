package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"strings"

	"mycelium/internal/myce"
)

// S04 adds only the offline conversion entry point. S08 owns the host CLI and
// its versioned command envelope; conversion outputs a private inventory.
func hostConvertCommand(args []string, out io.Writer) error {
	f := flag.NewFlagSet("host convert", flag.ContinueOnError)
	dir := f.String("host-state-dir", "", "offline shared host state directory")
	host := f.String("host-id", "", "explicit physical host mapping")
	dry := f.Bool("dry-run", false, "inventory only; do not publish state")
	apply := f.Bool("apply", false, "preserve originals and publish validated state offline")
	queues := map[string]string{}
	f.Func("queue-db", "repeat QUEUE_URL=OFFLINE_DB for every referenced queue", func(value string) error {
		queue, path, ok := strings.Cut(value, "=")
		if !ok || queue == "" || path == "" {
			return errors.New("queue-db requires QUEUE_URL=OFFLINE_DB")
		}
		if _, ok := queues[queue]; ok {
			return errors.New("duplicate queue mapping")
		}
		queues[queue] = path
		return nil
	})
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *dry == *apply || *dir == "" || *host == "" {
		return errors.New("usage: myce host convert --host-state-dir DIR --host-id ID --dry-run|--apply [--queue-db URL=DB]")
	}
	m, err := myce.ConvertHostControl(myce.HostConversionOptions{Dir: *dir, Host: *host, QueueDatabases: queues, Clock: myce.RealClock{}, Apply: *apply})
	if e := json.NewEncoder(out).Encode(m); e != nil {
		return e
	}
	return err
}
