#!/bin/sh
# Build on native Apple Silicon. Manifest signing can happen on its key's host.
set -eu
test "$(uname -s)/$(uname -m)" = Darwin/arm64 || { echo "native macOS arm64 release host required" >&2; exit 1; }
test "$#" = 2 || { echo "usage: scripts/build-release.sh VERSION OUTPUT_DIR" >&2; exit 1; }
test -n "${MYCE_DEVELOPER_ID:-}" || { echo "MYCE_DEVELOPER_ID is required for a native Darwin release; configure a provisioned Developer ID Application identity" >&2; exit 1; }
version=$1
output=$2
commit=$(git rev-parse HEAD)
mkdir -p "$output"
CGO_ENABLED=1 go build -trimpath -buildvcs=false -ldflags "-X mycelium/internal/myce.BuildVersion=$version -X mycelium/internal/myce.BuildCommit=$commit" -o "$output/myce-darwin-arm64" ./cmd/myce
"$output/myce-darwin-arm64" release build-targets > "$output/build-targets.txt"
while read -r os arch cgo; do
  CGO_ENABLED=$cgo GOOS=$os GOARCH=$arch go build -trimpath -buildvcs=false -ldflags "-X mycelium/internal/myce.BuildVersion=$version -X mycelium/internal/myce.BuildCommit=$commit" -o "$output/myce-$os-$arch" ./cmd/myce
done < "$output/build-targets.txt"
# Platform signing changes the bytes. It must precede manifest/hash generation.
codesign --force --options runtime --timestamp --identifier io.github.1kuna.mycelium --sign "$MYCE_DEVELOPER_ID" "$output/myce-darwin-arm64"
codesign --verify --strict -R='anchor apple generic and certificate leaf[field.1.2.840.113635.100.6.1.13] exists and identifier "io.github.1kuna.mycelium"' "$output/myce-darwin-arm64"
