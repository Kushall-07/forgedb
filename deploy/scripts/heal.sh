#!/usr/bin/env bash
# Reverse partition.sh: reconnect a node to forgedb-net and wait for the
# cluster to settle on a single leader again.
#
# Usage: deploy/scripts/heal.sh forgedb-1
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./lib.sh

svc="${1:?usage: heal.sh <forgedb-1|forgedb-2|forgedb-3>}"
echo "Reconnecting $svc to $NETWORK ..."
docker network connect "$NETWORK" "$svc"

echo "Waiting for the cluster to settle on a leader..."
leader="$(wait_for_leader 30)"
echo "Leader is now: $leader"
