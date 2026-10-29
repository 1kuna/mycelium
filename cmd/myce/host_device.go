package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/url"
	"strings"
	"time"

	"mycelium/internal/myce"
)

func hostDeviceCommand(ctx context.Context, args []string, out io.Writer) error {
	usage := errors.New("usage: myce host device disable HOST DEVICE [--reason TEXT] | enable HOST DEVICE [--config PATH]")
	if len(args) == 0 || args[0] != "disable" && args[0] != "enable" {
		return usage
	}
	f := flag.NewFlagSet("host device "+args[0], flag.ContinueOnError)
	authority := installedClientFlags(f)
	reason := f.String("reason", "", "operator reason for withholding this device")
	var flags, positional []string
	for i := 1; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "-") {
			flags = append(flags, arg)
			key := strings.TrimLeft(arg, "-")
			if (key == "reason" || key == "config" || key == "joining-file" || key == "queue" || key == "token") && i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
		} else {
			positional = append(positional, arg)
		}
	}
	if err := f.Parse(flags); err != nil {
		return err
	}
	if len(positional) != 2 || f.NArg() != 0 {
		return usage
	}
	reasonSet := false
	f.Visit(func(v *flag.Flag) { reasonSet = reasonSet || v.Name == "reason" })
	if args[0] == "enable" && reasonSet {
		return errors.New("--reason requires device disable")
	}
	c, err := authority.client()
	if err != nil {
		return err
	}
	if c.HTTP != nil {
		defer c.HTTP.CloseIdleConnections()
	}
	path := "/api/hosts/" + url.PathEscape(positional[0])
	var status myce.HostMaintenanceStatus
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = c.Call(readCtx, "GET", path+"/maintenance", nil, &status)
	cancel()
	if err != nil {
		return err
	}
	d, err := status.ResolveDevice(positional[1])
	if err != nil {
		return err
	}
	var revision uint64
	for _, p := range status.DeviceMaintenance {
		if p.Platform == d.Platform && p.Identity == d.Identity {
			revision = p.Revision
		}
	}
	request := myce.DeviceMaintenanceRequest{Action: args[0], Platform: d.Platform, Identity: d.Identity, Reason: *reason, IfRevision: &revision}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.Call(writeCtx, "POST", path+"/devices/maintenance", request, &status); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(status)
}
