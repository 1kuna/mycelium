#!/bin/sh
# Run on native Apple Silicon macOS with Go, Xcode CLT, and a provisioned key.
set -eu
test "$#" = 3 || { echo "usage: scripts/release.sh VERSION OUTPUT_DIR PRIVATE_SIGNING_KEY" >&2; exit 1; }
version=$1
output=$2
key=$3
sh "$(dirname "$0")/build-release.sh" "$version" "$output"
"$output/myce-darwin-arm64" release manifest --dir "$output" --version "$version" --signing-key "$key"
