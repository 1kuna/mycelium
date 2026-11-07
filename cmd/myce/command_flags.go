package main

import (
	"flag"
	"fmt"
	"strings"
)

// The catalog owns defaults and shared values; only the command's own set parses
// arguments. Sharing values also preserves aliases and f.Visit's explicit flags.
func commandFlagSet(command string, catalog *flag.FlagSet) *flag.FlagSet {
	sets := map[string]string{
		"queue":  `service-config discovery discovery-listen allow-downgrade release-manifest release-key release-url release-ca identity-dir host-config project-config debug-listen listen db upload-spool-dir log-retention upload-spool-bytes output-delivery-root file-store health-notify health-storage-delay file-retention file-store-production-reserve file-store-bytes checkpoints preemption-stop-grace production-stop-grace production-infrastructure-retries production-loss-timeout deadline-loss-timeout experiment-loss-timeout restart-grace environment-config model-config model-idle model-wait`,
		"join":   `service-config docker-host container-retention log-retention file-cache file-cache-bytes checkpoint-root extension-root job-file-root model-cache secret-root model-approval-root model-root native-read-root native-exec-root model-copy-parallel model-cache-bytes preemption-stop-grace production-stop-grace heartbeat memory-mode reserve-cpu-cores cpu-budget-cores host-memory-budget-gb memory-ceiling-slack-gb require-memory-ceilings memory-swap-max-bytes gpu-telemetry-helper reserve-host-memory-gb reserve-memory-gb state-dir name host-id device-settings device gpu memory-gb environments models environment-config environment-root environment-cache environment-cache-bytes model-config model-idle model-wait model-listen model-advertise enrollment curve-max-concurrency onboarding-benchmark daily-use laptop desktop max-jobs`,
		"dev":    `listen db checkpoints file-retention output-delivery-root fake-gpus production-infrastructure-retries file-store file-store-bytes file-store-production-reserve extension-root job-file-root host-config host-id project-config environment-config environment-cache-bytes model-config model-idle model-wait gpu environments models workers reserve-cpu-cores cpu-budget-cores memory-gb`,
		"submit": "config files-from",
		"usage":  "since by",
		"status": "config full json history",
		"cancel": "config project key",
		"leave":  "name now",
		"ledger": "config after high-water limit",
		"logs":   "config follow",
	}
	names := "token joining-file " + sets[command]
	if command != "queue" && command != "dev" {
		names += " queue"
	}
	f := selectCommandFlags(command, names, catalog)
	operands := map[string]string{
		"submit": " JOB.yaml", "status": " [JOB_ID]", "logs": " JOB_ID",
		"ledger": " JOB_ID", "cancel": " [JOB_ID]",
	}
	f.Usage = func() {
		fmt.Fprintf(f.Output(), "Usage of myce %s%s [flags]:\n", command, operands[command])
		if command == "dev" {
			fmt.Fprintln(f.Output(), "Or: myce dev -- COMMAND [ARG...] (temporary local queue; stops when COMMAND exits)")
		}
		if command == "cancel" {
			fmt.Fprintln(f.Output(), "Cancel by JOB_ID, or by --project PROJECT --key KEY.")
		}
		f.PrintDefaults()
	}
	return f
}

func selectCommandFlags(command, names string, catalog *flag.FlagSet) *flag.FlagSet {
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(catalog.Output())
	f.Usage = func() {
		fmt.Fprintf(f.Output(), "Usage of myce %s [flags]:\n", command)
		f.PrintDefaults()
	}
	for _, name := range strings.Fields(names) {
		v := catalog.Lookup(name)
		f.Var(v.Value, v.Name, v.Usage)
		f.Lookup(name).DefValue = v.DefValue
	}
	return f
}

// Older versions accepted the entire catalog for every command. Preserve that
// compatibility without advertising unrelated options. Ordinary invocations use
// only the command's set; a legacy invocation parses once with the old catalog
// so repeated values, aliases, explicit flags and positional parsing stay intact.
func parseCommandFlags(own, catalog *flag.FlagSet, args []string) (*flag.FlagSet, error) {
	parser := own
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" || arg == "-" || !strings.HasPrefix(arg, "-") {
			break
		}
		name, _, hasValue := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		option := own.Lookup(name)
		if option == nil {
			if catalog.Lookup(name) != nil {
				parser = catalog
			}
			break
		}
		boolean, ok := option.Value.(interface{ IsBoolFlag() bool })
		if !hasValue && !(ok && boolean.IsBoolFlag()) {
			i++
		}
	}
	catalog.Usage = own.Usage
	return parser, parser.Parse(args)
}
