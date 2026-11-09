package myce

// Reproduce a log prune owning the sole connection outside q.mu. An inactive
// instruction poll must neither queue for that connection nor block job reads.
