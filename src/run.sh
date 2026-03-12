#!/usr/bin/env bash
set -Eeuo pipefail

# Load version from .env if it exists and GITHUB_TARBALL_URL is not set
if [ -z "${GITHUB_TARBALL_URL:-}" ] && [ -f "$(dirname "$0")/../.env" ]; then
    source "$(dirname "$0")/../.env"
    GITHUB_TARBALL_URL="https://github.com/VikingTheDev/inf3203/releases/download/${VERSION}/deploy-x86_64-unknown-linux-musl.tar.gz"
fi

GITHUB_TARBALL_URL="${GITHUB_TARBALL_URL:-https://github.com/VikingTheDev/inf3203/releases/download/v0.2.8/deploy-x86_64-unknown-linux-musl.tar.gz}"

usage() {
    echo "Usage: $0 <nodes>"
    echo "Set GITHUB_TARBALL_URL to the .tar.gz for the script to work env variable or in the script"
    exit 1
}

# --- Check that nodes argument exists ---
if [ $# -ne 1 ]; then
    usage
fi

NODES="$1"

# --- check that nodes is a number between 1 and 100 ---
if ! [[ "$NODES" =~ ^[0-9]+$ ]] || (( NODES < 1 || NODES > 100 )); then
    echo "Error: Nodes must be a number between 1 and 100" >&2
    exit 2
fi

# --- workspace & cleanup ---
WORKDIR="$(mktemp -d)"
TARBALL="$WORKDIR/binary.tar.gz"
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT


# --- download tarball ---
echo "Downloading Deploy Tarball from GitHub"
curl -fsS --no-progress-meter -L "$GITHUB_TARBALL_URL" -o "$TARBALL"
[[ -s "$TARBALL" ]] || { echo "Error: Download failed or empty file." >&2; exit 3; }

# --- ensure binary exists ---
mapfile -t entries < <(tar -tzf "$TARBALL")
BIN="${entries[0]}"
[[ -n "$BIN" ]] || { echo "Error: Could not find binary in tarball." >&2; exit 4; }

# -- extract and make binary executable ---
tar -xzf "$TARBALL" -C "$WORKDIR"
BINPATH="$WORKDIR/$BIN"
chmod +x "$BINPATH"

exec "$BINPATH" "$NODES"

echo "Servers started, exiting master script"
exit 0