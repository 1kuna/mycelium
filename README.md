# Mycelium

A shared GPU job queue for all of maintainer's projects. One queue service runs on the B70's CPU, and every machine that wants work runs a small worker. Projects submit jobs instead of owning machines.

The redesign is in progress. See `docs/design-proposal.md` once it has been approved.

The previous version, a peer-to-peer inference control plane, was archived on 2026-10-08. It lives at git tag `v1-final`:

    git checkout v1-final
