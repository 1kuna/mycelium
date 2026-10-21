#!/bin/sh
# Run on native Apple Silicon macOS with Go, Xcode CLT, and a provisioned key.
set -eu
test "$(uname -s)/$(uname -m)" = Darwin/arm64 || { echo "native macOS arm64 release host required" >&2; exit 1; }
test "$#" = 3 || { echo "usage: scripts/release.sh VERSION OUTPUT_DIR PRIVATE_SIGNING_KEY" >&2; exit 1; }
version=$1
output=$2
key=$3
mkdir -p "$output"
for arch in amd64 arm64; do
  CGO_ENABLED=0 GOOS=linux GOARCH=$arch go build -trimpath -ldflags "-X mycelium/internal/myce.BuildVersion=$version" -o "$output/myce-linux-$arch" ./cmd/myce
done
CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags "-X mycelium/internal/myce.BuildVersion=$version" -o "$output/myce-darwin-arm64" ./cmd/myce
"$output/myce-darwin-arm64" release manifest --dir "$output" --version "$version" --signing-key "$key"
