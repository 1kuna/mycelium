// Package skills embeds the consumer agent skill, so every myce binary carries
// the guide that matches its own release.
package skills

import "embed"

// Mycelium holds skills/mycelium; paths inside it start with "mycelium/".
//
//go:embed mycelium
var Mycelium embed.FS
