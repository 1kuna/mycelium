package myce

// The dev composition supplies its real HTTP API, queue and command runner.
// Inventory, scheduling time, checkpoint availability and the service manager
// are fake. The legacy leg speaks the supported singular wire form and executes
// through Worker.start.
