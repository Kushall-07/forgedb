#!/usr/bin/env bash
# Isolate one node from forgedb-net: the process stays alive, but its
# network path to every peer (and to the host's published ports) is cut
# -- a real partition, not a crash (see deploy/README.md's "crash vs.
# partition" section). Reverse with heal.sh.
#
# Usage: deploy/scripts/partition.sh forgedb-1
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./lib.sh

svc="${1:?usage: partition.sh <forgedb-1|forgedb-2|forgedb-3>}"
echo "Disconnecting $svc from $NETWORK ..."
docker network disconnect "$NETWORK" "$svc"
echo "$svc is now partitioned (process still running; unreachable over the network)."
