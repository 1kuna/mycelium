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

**capability profile**:
What a hypha can actually do, measured by the onboarding benchmark: its resources, their sizes and memory layout, and how fast it does each kind of work.
_Avoid_: specs, inventory, hardware info

**onboarding benchmark**:
The one-time, full-system measurement a hypha runs when it joins, and again when its hardware changes, to build its capability profile.
_Avoid_: calibration, probe

### Work

**job**:
One piece of work handed to a mycelium, described either as a kind of work or as a program, with no mention of hardware.
_Avoid_: task, request, submission

**kind of work**:
A recognized type of job, such as transcribing audio with a given model, that extensions know how to run.
_Avoid_: workload type, task type, operation

**program**:
A job given as something to execute, such as a script, container or executable, for work no kind of work covers yet.
_Avoid_: raw job, custom job, command job

**similarity**:
How closely a job's output on one hypha matches a native reference, measured in the way that suits its kind of work.
_Avoid_: accuracy, parity, exactness

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
