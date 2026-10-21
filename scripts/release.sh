#!/bin/sh
# Run on native Apple Silicon macOS with Go, Xcode CLT, and a provisioned key.
set -eu
test "$(uname -s)/$(uname -m)" = Darwin/arm64 || { echo "native macOS arm64 release host required" >&2; exit 1; }
test "$#" = 3 || { echo "usage: scripts/release.sh VERSION OUTPUT_DIR PRIVATE_SIGNING_KEY" >&2; exit 1; }
version=$1
output=$2
key=$3
mkdir -p "$output"
# Build the native publisher first; it supplies the validator's target matrix.
CGO_ENABLED=1 go build -trimpath -ldflags "-X mycelium/internal/myce.BuildVersion=$version" -o "$output/myce-darwin-arm64" ./cmd/myce
"$output/myce-darwin-arm64" release build-targets > "$output/build-targets.txt"
while read -r os arch cgo; do
  CGO_ENABLED=$cgo GOOS=$os GOARCH=$arch go build -trimpath -ldflags "-X mycelium/internal/myce.BuildVersion=$version" -o "$output/myce-$os-$arch" ./cmd/myce
done < "$output/build-targets.txt"
"$output/myce-darwin-arm64" release manifest --dir "$output" --version "$version" --signing-key "$key"
