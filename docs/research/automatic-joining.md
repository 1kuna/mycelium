# Finding and joining hyphae automatically

Research for [issue #23](https://github.com/1kuna/mycelium/issues/23), under
[map #17](https://github.com/1kuna/mycelium/issues/17). Prepared 2026-10-14.
All primary sources below were read on 2026-10-14; publication dates are given
where known, otherwise the source is marked as living documentation.
This is a research recommendation, not an approved spec or implemented behavior.

## Question

How can installing Mycelium on macOS, Linux, or Windows/WSL2 find the existing
mycelium and join automatically, safely, and without restoring v1 complexity?
How should it later add approve-to-join and individually revocable hypha keys?
These are the ticket's questions. The latest vision correction explicitly says
automatic joining initially, with approval and per-machine keys when other
people's machines become eligible; it supersedes the earlier approval-first
decision. [P1, P2]

## Short answer

Use standard **mDNS with DNS-SD** to find the spore's current queue service,
then join over a separately authenticated connection. Keep an explicit or saved
queue address as the escape hatch. DNS-SD already supplies service enumeration,
destination, port, and metadata; it does not establish ownership or permission.
This adds discovery to the existing one-binary, one-SQLite design without a peer
network. [S1, S2, P3]

**Automatic joining is compatible with no approval step, but safe owner-only
joining needs prior trust.** Recommend privately provisioning the mycelium's
enrollment credential and trusted spore identity through the installation
workflow. A universal public installer plus unauthenticated discovery cannot
distinguish the owner's machines from another computer on the same link.
Advertising a token or silently trusting the first response is not a safe fix.
This is an inference from mDNS's explicit trust limitations and the enrollment
patterns below, not a request to reinstate the rejected approval step. [S1 §21,
S9, S11, P2]

Do not promise discovery on every WSL2 installation: NAT is the default;
mirrored networking adds multicast on eligible Windows versions, with firewall
requirements. A configured endpoint remains necessary as a fallback. [S6, S7]

## Findings

### 1. Today's join has authority, but not automatic service finding

The reviewed main snapshot is `b989acc138fc160c60430855e415ea3846048b44`.
It uses one Go binary, a local SQLite queue, and command-running processes on
participating machines. Joining requires a queue address and the shared token.
Bare `myce join` auto-detects GPU inventory; that is not automatic discovery of
a mycelium. Linux installation uses systemd user units and macOS uses launchd.
The operations guide describes a token-protected HTTP API and explicitly does
not provide TLS in those recipes. Therefore “discover and send the existing
token” alone does not produce an authenticated, encrypted joining design.
[P3, P4, P5]

The glossary calls one participating machine a **hypha**, the whole installation
a **mycelium**, and its first hypha a **spore**. The initial proposal can keep
the queue at the spore without treating the spore as an eternal authority:
future survivability is a separate map decision. Joining offers a hypha's
**resources**; it does not make an **extension** from the **extension library**
**qualified** on that hypha. Qualification concerns correct output and is
distinct from identity and admission. [P6, P1]

### 2. DNS-SD covers the discovery problem already

mDNS resolves names on the local link using UDP 5353 and link-local multicast.
DNS-SD uses PTR records to enumerate services, SRV records for their target and
port, and TXT records for small structured metadata. DNS-SD works with both
mDNS and conventional unicast DNS. A machine on another routed segment should
not be assumed discoverable through ordinary link-local mDNS. [S1 §§3, 5, 11,
S2 §§1, 4–6]

**Proposed minimal advertisement:** one queue-service instance, with a proposed
service type such as `_mycelium._tcp`, protocol version, opaque mycelium ID,
and enrollment mode. These are hints. Validate the actual endpoint and protocol
over the authenticated connection; never put the shared token, private key,
personal machine name, job information, or resource inventory in TXT records.
The exact service name remains a design choice, not an asserted registered
standard. This uses DNS-SD's existing record model and keeps discovery separate
from authority. [S2 §§5–7, S1 §21, S9]

mDNS's security section assumes cooperative participants and explicitly warns
that hostile computers on the same physical link require a way to distinguish
trusted messages. A mycelium ID, an advertised certificate fingerprint, or a
self-signed certificate received through that same untrusted discovery channel
cannot supply an independent trust anchor. Encryption with a blindly accepted
key protects against passive reading but does not prove the intended spore.
[S1 §21; inference, illustrated by S9's preset fingerprint list]

### 3. Platform facilities exist; packaging still needs care

| Platform | Primary-source fact | Implication proposed for Mycelium |
|---|---|---|
| macOS | Bonjour publishes, browses, and resolves local services. [S3] | Use DNS-SD; keep foreground installation and unattended service startup as distinct acceptance cases. |
| Linux | Avahi implements mDNS/DNS-SD, primarily through D-Bus, and can publish service definitions. [S4] | Reuse it where present; depending on it adds an OS service/package prerequisite. |
| Native Windows | `DnsServiceBrowse` provides asynchronous local DNS-SD browsing, with Windows 10 as its documented minimum desktop client. [S5] | A native discovery helper is possible later; native Windows execution remains outside this initial WSL2 path. [P1, P2] |
| WSL2 | Default NAT differs from mirrored mode, which explicitly adds multicast. [S6] | Linux-side discovery cannot be promised for default NAT; retain the fallback described below. |

The one-binary alternative is an existing Go mDNS/DNS-SD library rather than a
new packet protocol. HashiCorp's `mdns` repository demonstrates both publishing
and lookup and warns that multicast restrictions prevent discovery. Its README
is evidence that reusable code exists, **not** evidence that this particular
dependency meets every interoperability, maintenance, or packaging requirement.
Prefer evaluating a Go library first to preserve the single-binary experience;
use OS backends if documented or reproduced interoperability problems require
them. Do not build custom multicast machinery merely to avoid evaluation.
[S12, P3; recommendation]

macOS 15 introduced local network privacy. Apple's TN3179 says Terminal-launched
command-line tools are automatically allowed, as are launchd **daemons**, but
that daemon exception does **not** apply to launchd **agents**. The current
user-service packaging therefore needs its own acceptance check; success from
a terminal does not establish success after login or reboot. The OS may require
local-network permission even though Mycelium itself has no join-approval step.
Do not claim an unconditional no-prompt experience or recommend running as root
to evade privacy controls. [S13, P4; recommendation]

### 4. WSL2 and firewalls are the main “just works” qualification boundary

Microsoft documents mirrored mode on Windows 11 22H2 and later, enabled through
the WSL configuration's `networkingMode=mirrored`. Its advertised benefits
include multicast, IPv6, and direct LAN connectivity. WSL 2.0.9 and later has
Hyper-V firewall enabled by default on those Windows versions. Mirrored mode is
therefore an eligible path, not proof that packets pass on a particular machine.
[S6]

**Recommended WSL2 order:** reuse a saved authenticated endpoint; otherwise
browse directly when mirrored mode is available; on NAT or failed multicast,
accept a privately supplied queue endpoint through installation. A later small
Windows-side DNS-SD helper could pass discovery results to WSL, but that adds a
second packaging boundary and must still use the same enrollment trust. Do not
add port forwarding just for outbound joining, disable the firewall, or silently
alter WSL's networking mode. Today's participating processes pull work from the
queue; inbound reachability to every hypha is not an initial requirement.
[S5–S7, P3; recommendation]

Firewall planning must distinguish **discovery** from **joining**: mDNS needs
UDP 5353 multicast traffic on the participating interface; the queue also needs
its actual TCP API port reachable. Hyper-V firewall supports specific rules and
network profiles. Recommend the narrow needed rules on the home/private profile,
with no broad default-inbound allow, router exposure, or multicast reflector by
default. Guest isolation, segmented networks, or multicast filtering can prevent
discovery; an explicit endpoint avoids the multicast dependency but cannot bypass
blocked TCP connectivity. A discovery timeout cannot prove which of those causes
occurred. [S1, S7, S12; inference and recommendation]

### 5. Mature tools automate logistics after establishing authority

- **Syncthing:** its local discovery protocol sends UDP announcements containing
  identity and connection information. Actual connections use TLS, and the
  certificate fingerprint must match a preset acceptable-machine list. Initial
  setup requires mutual identity configuration. Lesson: finding a machine and
  trusting it are different operations; borrow that separation, not its peer
  topology or synchronization machinery. [S8–S10]
- **Tailscale:** enrollment auth keys allow registration without browser login.
  Keys can be single-use or reusable; pre-approved keys avoid an additional
  approval step. Each enrolled machine has a separate identity key. Revoking an
  enrollment auth key does not deauthorize machines already enrolled with it.
  Lesson: unattended enrollment can consume previously provisioned authority,
  and enrollment revocation differs from hypha revocation. This is a pattern to
  borrow, not a requirement to add Tailscale or cloud coordination now. [S11, S14]
- **Matter:** Google's commissioning documentation describes discovery followed
  by a passcode-authenticated initial session, installation of operational
  credentials, then DNS-SD discovery and authenticated operational sessions.
  Lesson: discovery is a locator, and a separate onboarding secret establishes
  initial trust. Its commissioning machinery is excessive for Mycelium's present
  scope. [S15; recommendation]

None of these sources establishes safe owner-only enrollment from a generic
public installer with no prior authority. The concrete unresolved product choice
is **how the one install command receives private trust material**, not which
multicast packet format to invent. [S1 §21, S9, S11, S15; inference]

## Options and trade-offs for Mycelium

The following are research judgments based on the cited mechanisms, not measured
performance or an approved implementation plan.

| Option | Benefit | Cost / limit | Judgment |
|---|---|---|---|
| mDNS/DNS-SD + installation-provisioned trust | Finds changing endpoints on a reachable local link; no Mycelium approval step. [S1–S2, S11] | Needs a private way to provision authority; multicast and OS permissions can fail. [S6, S13] | Recommended default. |
| Explicit/saved endpoint + the same trust | Reuses the current queue-address model and avoids multicast. [P4] | Initial address must be supplied; stale addresses still need recovery. | Essential fallback. |
| Conventional DNS-SD | Same service model can work through conventional DNS. [S2] | Requires DNS administration; not zero configuration for a typical home. | Optional when already available. |
| First-contact trust on an assumed owner-only LAN | Generic installation can join without prior credentials. | Another LAN participant can impersonate the spore or enroll; first-contact pinning protects only subsequent continuity. [S1 §21, S9; inference] | Cannot satisfy authenticated owner-only joining. Surface the weaker threat model rather than calling it safe. |
| Custom broadcast or address scanning | Syncthing demonstrates that another announcement format is possible. [S8] | New protocol and packet handling; does not solve ownership, multicast restrictions, or firewalls. [S1, S6, S12] | No named requirement justifies it. |
| Windows-side finding helper | Uses Windows DNS-SD while execution remains in WSL2. [S5] | Additional executable/bridge and installation lifecycle; no end-to-end verification here. | Reserve for a demonstrated NAT discovery requirement. |
| Tailscale endpoint later | Matches the map's future outside-home direction. [P1] | Additional setup and trust domain; not ordinary home-link mDNS. [S1, S11] | Defer; retain discovery/enrollment separation. |

## Recommendation and migration path

**Recommend DNS-SD plus prior private trust, with automatic admission initially.**
Keep discovery a small input to the existing join path, not a new membership
system. The following proposed sequence preserves the vision's no-approval
initial behavior and the approved design's simplicity. [P1–P3, S1–S2]

1. **Initial install:** the installation workflow privately provisions the
   mycelium identity, a trusted spore key/certificate, and enrollment authority.
   A saved endpoint or DNS-SD locates the queue. Validate its identity before
   sending any shared bearer token; then admit automatically and persist the
   identity separately from its changeable address. Never retrieve the token
   from an unauthenticated discovered endpoint or publish it in an install URL,
   command arguments, logs, or advertisements. This requires adding authenticated
   transport to the current HTTP-only recipe; discovery alone is insufficient.
   [P4, S1 §21, S9, S11; proposal]
2. **Ambiguity and recovery:** choose only a candidate matching provisioned
   trust, not the fastest response or nicest name. If none matches, explain
   “cannot find your mycelium” or “identity does not match”; do not create another
   spore or join an arbitrary candidate. Reconnect to the pinned identity after
   sleep or address changes. Try the saved endpoint, then bounded discovery;
   keep retrying without treating timeout as permission to change membership.
   Honor an existing disabled state. These are proposed behavioral safeguards,
   grounded in discovery's non-authoritative nature and current disable/recovery
   semantics. [S1, S2, P4]
3. **Later, trusted people's hyphae:** add an enrollment request containing a
   locally generated public key and a pending state in the existing SQLite
   database. An authenticated owner action approves the key; an approved hypha
   authenticates through its own key/certificate and can be revoked individually.
   Keep discovery unchanged. Approval must bind the displayed identity to the
   requested key; a display name is insufficient. Use established TLS and
   cryptographic libraries, not custom cryptographic proofs. [P2, S9, S14; proposal]
4. **Retire shared access deliberately:** distinguish enrollment credentials,
   hypha credentials, and submission/admin authority. Migrate existing hyphae
   through authenticated enrollment, track which have converted, then disable
   legacy shared-token access for the participating-machine APIs. Revocation
   must reject new sessions and stop further assignments on existing sessions;
   define running-work cleanup separately. Rotating an enrollment token alone
   is not individual revocation. Exact rollout and cleanup contracts need a
   separate approved design. [P2, S11, S14, P4; proposal]

**Decision to carry back to the orchestrator (`needs-maintainer`):** choose the private
installation-provisioning path before promising secure, fully automatic joining.
Recommended reading of “one command” is one command consuming privately supplied
trust, without a Mycelium approval prompt. If it instead means the exact same
public command with no prior trust, explicitly accept an owner-only-link threat
model or revise that requirement; the cited mechanisms do not establish machine
ownership from LAN presence. Revisit when writing the next approved spec, before
implementation. No approval-first behavior is silently substituted. [P2, S1 §21,
S11; research conclusion]

## Confidence and what was not verified

**High confidence:** DNS-SD's record model, mDNS's link scope and trust limits,
Microsoft's documented NAT/mirrored distinction, and mature tools' separation of
discovery and authenticated admission. **Moderate confidence:** the recommended
single-binary packaging and private-install workflow will meet the intended ease
of use; neither was implemented or tested. [S1–S15]

No experiments, source changes, service installation, credential access, SSH, or
real-machine access were performed. This report does not verify the actual home
network, multicast forwarding, firewall rules, VPN interactions, WSL version,
mirrored mode, macOS launch-agent permissions, or behavior after sleep/reboot.
It does not select or qualify a Go dependency, prove zero prompts across OS
versions, or specify how private installer material is distributed and revoked.
Those remain acceptance work, not facts inferred from documentation.

Proposed future laptop-only acceptance work: fake discovery results for no,
one, and multiple candidates; an impersonating candidate; expired authority;
identity mismatch; stale endpoint; disabled-state retention; and the transition
from automatic admission to pending/approved/revoked. Scheduling/retry decisions
must use the project's injected Clock. Separately exercise OS discovery and
service-start permissions on each supported platform before claiming that joining
“just works.” No extension qualification or GPU is needed to check membership.
[P3, P4, P6; proposal only]

## Current state and next step

Research complete; documented options and recommendation only. Issue #23 remains
open for the orchestrator. Next: resolve private installation trust provisioning,
then incorporate the joining contract into the next approved design under #17.
No production or main-branch change is proposed by this research branch. [P1, P2]

## Primary sources

Project sources are pinned where possible; issues are living decision records.
Every external living source was accessed 2026-10-14. No secondary reporting is
used as evidence.

- **P1:** [Map #17](https://github.com/1kuna/mycelium/issues/17), project decision record, 2026-10-14.
- **P2:** [Vision #16 and comments](https://github.com/1kuna/mycelium/issues/16), 2026-10-14; especially [the automatic-joining correction](https://github.com/1kuna/mycelium/issues/16#issuecomment-6175319939).
- **P3:** [Approved design](https://github.com/1kuna/mycelium/blob/b989acc138fc160c60430855e415ea3846048b44/docs/design-proposal.md), approved 2026-10-08, reviewed main snapshot above.
- **P4:** [Operations](https://github.com/1kuna/mycelium/blob/b989acc138fc160c60430855e415ea3846048b44/docs/operations.md), project documentation at the reviewed snapshot.
- **P5:** [Start here](https://github.com/1kuna/mycelium/blob/b989acc138fc160c60430855e415ea3846048b44/docs/START-HERE.md), project documentation at the reviewed snapshot.
- **P6:** [Glossary](https://github.com/1kuna/mycelium/blob/2587b223b8d711080fb4a6ace3182a6e56fa0b62/GLOSSARY.md), reviewed `docs/mycelium-vision` snapshot.
- **S1:** IETF [RFC 6762: Multicast DNS](https://www.rfc-editor.org/rfc/rfc6762.html), February 2013; §§3, 5, 11, 21.
- **S2:** IETF [RFC 6763: DNS-Based Service Discovery](https://www.rfc-editor.org/rfc/rfc6763.html), February 2013; §§1, 4–7, 15.
- **S3:** Apple [Bonjour](https://developer.apple.com/bonjour/), living developer documentation.
- **S4:** Avahi project [What is Avahi?](https://avahi.org/), living project documentation.
- **S5:** Microsoft [DnsServiceBrowse](https://learn.microsoft.com/en-us/windows/win32/api/windns/nf-windns-dnsservicebrowse), living API documentation, Remarks and Requirements.
- **S6:** Microsoft [Accessing network applications with WSL](https://learn.microsoft.com/en-us/windows/wsl/networking), living documentation; default NAT, mirrored mode, and firewall sections.
- **S7:** Microsoft [Hyper-V firewall](https://learn.microsoft.com/en-us/windows/security/operating-system-security/network-security/windows-firewall/hyper-v-firewall), living documentation; specific rules and profiles.
- **S8:** Syncthing [Local Discovery Protocol v4](https://docs.syncthing.net/specs/localdisco-v4.html), living protocol specification.
- **S9:** Syncthing [Security Principles](https://docs.syncthing.net/users/security.html), living documentation; TLS and acceptable certificate fingerprints.
- **S10:** Syncthing [Getting Started](https://docs.syncthing.net/intro/getting-started.html), living documentation; exchanging identities.
- **S11:** Tailscale [Auth keys](https://tailscale.com/docs/features/access-control/auth-keys), living documentation; unattended registration, key types, expiry, and revocation.
- **S12:** HashiCorp [mdns README](https://github.com/hashicorp/mdns/blob/master/README.md), living first-party repository documentation; publishing, lookup, and multicast limitations.
- **S13:** Apple [TN3179: Understanding local network privacy](https://developer.apple.com/documentation/technotes/tn3179-understanding-local-network-privacy), living technical note; macOS introduction and command-line/daemon/agent distinctions.
- **S14:** Tailscale [Device approval](https://tailscale.com/docs/features/access-control/device-management/device-approval), living documentation; pending access, pre-approval, and deauthorization.
- **S15:** Google [Matter commissioning](https://developers.home.google.com/matter/primer/commissioning), first-party implementation documentation, updated 2022-07-13; discovery, PASE, operational credentials, and CASE.
