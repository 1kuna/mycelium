package myce

// Mixed shortages must describe both eligible hosts, regardless of whether
// the CPU-busy host also lacks RAM. The RAM fallback used to skip busy CPUs.

// A RAM shortage on another eligible host must not hide the occupied card on
// the host that has enough RAM (#537). Cover both shared and exclusive slots.
