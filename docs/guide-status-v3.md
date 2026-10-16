# Seeing your mycelium

Run `myce status` using your usual queue address and private token. `--full`
shows full job IDs. Open the queue address in a browser for the same information;
sign in as `myce`, using the token as the password. The page is read-only and
refreshes every five seconds. Keep tokens in private configuration, not URLs.

Each hypha shows its capability profile: CPU count, system memory, accelerator
memory, shared or separate memory, and local storage. Missing measurements say
unknown. Extension reports say which kind of work or program is qualified on
that hypha; having hardware alone does not make an extension qualified.

Availability explains owner use, battery power, heat, missing readings,
disabling, leaving, and loss of contact. An available hypha can still be busy.
Running jobs show their hypha and attempt. Every waiting job has an explanation,
including missing qualification, memory or resource requirements, unavailable
hyphae, software preparation, and work ahead in the queue.

The page and CLI use the queue's decisions. Reading status does not choose where
to run work. The status API returns job summaries without submitted commands,
environment values, program arguments, extension files, or local checkpoint
paths. Use the existing job output and log commands when you need their results.

For maintenance gates, every nonterminal job in redacted `GET /api/status`
retains `spec.allowed_machines` (machine names) and reports one of:

- `eligible_hosts`: sorted, unique host IDs of currently known machines allowed
  by both the job and project allow-lists. An empty array means no known match.
  Workers without an explicit host ID use their machine ID.
- `eligible_unrestricted: true`, with `eligible_hosts` omitted: neither allow-list
  restricts the job. Treat it as able to run anywhere, including future hosts.

These fields cover queued, infra-requeued, running and stopping jobs. They
express placement policy, not current dispatch readiness: busy, disabled, offline
or temporarily unavailable machines remain candidates under that policy.
Unknown machine names remain in `spec.allowed_machines` but contribute no host
until they join. Refresh status before each decision. A gate may ignore a
restricted job only when its protected host is absent from `eligible_hosts`;
unrestricted jobs, missing/invalid eligibility fields and unreadable status must
fail closed. Also check current assignments (`machine` / `production_host`) and
release/hold state before maintenance: policy eligibility alone proves no release
of work already on a host. Terminal jobs omit both derived eligibility fields.
