package myce

// Keep the prior CPU/I/O wire contract intact with a queue that supports v1 but
// has a strict decoder predating CPU pressure. Every worker delivery shape and
// a live downgrade preserve durable local evidence while stripping only fields
// unsupported by that queue.
