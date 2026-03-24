#!/usr/bin/env bash
# run.sh — downloads the Áika deploy binary and runs it.
#
# Run this from a directory on the shared cluster filesystem (e.g. your NFS
# home directory) so the deploy binary can install aika-node there and all
# cluster nodes can reach it.
#
# Usage:
#   bash run.sh <num_nodes>
#
# The deploy binary handles everything: it downloads aika-node once to $HOME,
# discovers available nodes, and SSH-starts the cluster directly.
set -Eeuo pipefail

DEPLOY_URL="${DEPLOY_URL:-https://github.com/VikingTheDev/inf3203/releases/download/v0.1.0/deploy-x86_64-unknown-linux-musl.tar.gz}"

if [ $# -ne 1 ]; then
    echo "Usage: $0 <num_nodes>" >&2
    exit 1
fi

WORKDIR="$(mktemp -d)"
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

echo "Downloading deploy binary…"
curl -fsS --no-progress-meter -L "$DEPLOY_URL" -o "$WORKDIR/deploy.tar.gz"
tar -xzf "$WORKDIR/deploy.tar.gz" -C "$WORKDIR"
chmod +x "$WORKDIR/deploy"

exec "$WORKDIR/deploy" "$1"