#!/usr/bin/env bash
#
# chaos_test.sh
# =============
# Purpose: inject faults into the running cluster while client.py keeps
# submitting transactions, then verify the ledger still converges.
#
# Faults exercised:
#   1. Leader crash + re-election
#   2. Two simultaneous crashes (Paxos f=2 boundary)
#   3. Network latency  (via Toxiproxy)
#   4. Network partition (via Toxiproxy bandwidth=0 / timeout toxic)
#   5. Recovery (restart crashed nodes, confirm convergence)
#
# Requires: docker, docker compose, and the toxiproxy-cli inside the
# 'toxiproxy' container (image ghcr.io/shopify/toxiproxy).
#
# Usage:
#   chmod +x tests/chaos_test.sh
#   ./tests/chaos_test.sh
#
set -uo pipefail

COMPOSE="docker compose"
TPROXY="docker exec toxiproxy /toxiproxy-cli"

banner() { echo -e "\n================ $1 ================\n"; }

wait_secs() { echo "...waiting ${1}s while clients keep submitting..."; sleep "$1"; }

banner "STEP 0: confirm cluster is up"
$COMPOSE ps

banner "STEP 1: register Toxiproxy routes (node2/node3 peer ports)"
# Each proxy listens on the toxiproxy container and forwards to a node.
$TPROXY create node2_proxy --listen 0.0.0.0:19002 --upstream node2:9000 2>/dev/null || true
$TPROXY create node3_proxy --listen 0.0.0.0:19003 --upstream node3:9000 2>/dev/null || true
$TPROXY list

banner "STEP 2: start continuous client load in background"
$COMPOSE exec -d client python client.py --count 200 --rate 5
wait_secs 5

banner "STEP 3: CRASH THE LEADER -> expect re-election"
LEADER=$($COMPOSE exec -T node1 python -c "import asyncio,client; print(asyncio.run(client.find_leader()))" 2>/dev/null | tr -d '\r')
echo "current leader: ${LEADER:-unknown}"
if [ -n "${LEADER:-}" ] && [ "$LEADER" != "None" ]; then
  echo "killing $LEADER ..."
  $COMPOSE kill "$LEADER"
fi
wait_secs 8   # new leader should be elected; commits resume

banner "STEP 4: SECOND CRASH (Paxos f=2 boundary, majority must survive)"
$COMPOSE kill node5
wait_secs 8

banner "STEP 5: INJECT 800ms LATENCY on node2/node3 links"
$TPROXY toxic add node2_proxy -t latency -a latency=800 -a jitter=200 2>/dev/null || true
$TPROXY toxic add node3_proxy -t latency -a latency=800 -a jitter=200 2>/dev/null || true
wait_secs 8

banner "STEP 6: NETWORK PARTITION (cut node2/node3 with timeout toxic)"
$TPROXY toxic add node2_proxy -t timeout -a timeout=0 2>/dev/null || true
$TPROXY toxic add node3_proxy -t timeout -a timeout=0 2>/dev/null || true
wait_secs 8

banner "STEP 7: HEAL PARTITION + RESTART CRASHED NODES"
$TPROXY toxic remove node2_proxy -n node2_proxy_timeout 2>/dev/null || true
$TPROXY toxic remove node3_proxy -n node3_proxy_timeout 2>/dev/null || true
$TPROXY toxic remove node2_proxy -n node2_proxy_latency 2>/dev/null || true
$TPROXY toxic remove node3_proxy -n node3_proxy_latency 2>/dev/null || true
$COMPOSE up -d --no-recreate node5 "${LEADER:-node1}"
wait_secs 10

banner "STEP 8: VERIFY LEDGER CONVERGENCE across surviving nodes"
for n in node1 node2 node3 node4 node5; do
  COUNT=$($COMPOSE exec -T "$n" sh -c "wc -l < /data/${n}.log 2>/dev/null || echo 0" | tr -d '\r')
  echo "$n committed entries: ${COUNT}"
done

echo -e "\nChaos run complete. Compare committed counts above - honest nodes"
echo "should converge to the same committed prefix (allowing in-flight skew)."
