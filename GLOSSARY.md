# Mycelium

Mycelium turns all of a person's computers, however different, into one shared pool of compute that runs their heavy work wherever there's room.

## Language

### The whole and its parts

**Mycelium** (the software):
The program, written with a capital M, that a person installs on their machines.

**mycelium** (an installation):
One person's machines joined together into a single pool of compute, as in "my mycelium". A singular noun.
_Avoid_: cluster, organism, fleet, network

**hypha** (plural **hyphae**):
One machine that has joined a mycelium and offers its resources to it.
_Avoid_: node, member, worker machine

**spore**:
The first hypha of a mycelium, the one it grew from. It doesn't have to stay in charge: a mycelium is meant to survive losing any of its hyphae.
_Avoid_: hub, master, coordinator, primary

**resource**:
A kind of capacity a hypha offers: GPU, CPU, memory, storage, network, or an accelerator such as a neural engine.
_Avoid_: device, capability

### Extending what can run

**extension**:
A reusable add-on that teaches Mycelium to run one kind of work on one kind of resource. Written once, then shared.
_Avoid_: plugin, adapter, port, driver

**extension library**:
The shared collection of extensions that any mycelium can draw from.
_Avoid_: marketplace, registry, store

**qualified**:
Said of an extension that Mycelium has checked produces correct output on a particular hypha.
_Avoid_: certified, verified, supported
