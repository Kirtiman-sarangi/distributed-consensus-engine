#!/usr/bin/env bash
set -e

banner() {
  echo ""
  echo "================ $1 ================"
  echo ""
}

TP="docker exec toxiproxy /toxiproxy-cli"

banner "STEP 0: confirm cluster is up"
docker compose ps

banner "STEP 1: register Toxiproxy routes"
$TP delete node2_proxy >/dev/null 2>&1 || true
$TP delete node3_proxy >/dev/null 2>&1 || true

$TP create --listen 0.0.0.0:19002 --upstream node2:9000 node2_proxy || true
$TP create --listen 0.0.0.0:19003 --upstream node3:9000 node3_proxy || true

$TP list || true

banner "STEP 2: start continuous client load in background"
docker compose rm -f client >/dev/null 2>&1 || true
docker compose up -d client
sleep 5

banner "STEP 3: detect and crash current leader"
LEADER=$(docker compose exec -T node1 python -c "import asyncio,client; print(asyncio.run(client.find_leader()))" | tr -d '\r')
echo "current leader: $LEADER"
docker compose kill "$LEADER"
sleep 8

banner "STEP 4: crash second node to test Paxos f=2 boundary"
SECOND="node5"
if [ "$LEADER" = "node5" ]; then
  SECOND="node4"
fi
echo "second crashed node: $SECOND"
docker compose kill "$SECOND"
sleep 8

banner "STEP 5: inject 800ms latency using Toxiproxy"
$TP toxic remove -n node2_latency node2_proxy >/dev/null 2>&1 || true
$TP toxic remove -n node3_latency node3_proxy >/dev/null 2>&1 || true

$TP toxic add -n node2_latency -t latency -a latency=800 node2_proxy || true
$TP toxic add -n node3_latency -t latency -a latency=800 node3_proxy || true

$TP inspect node2_proxy || true
$TP inspect node3_proxy || true
sleep 8

banner "STEP 6: inject timeout partition using Toxiproxy"
$TP toxic remove -n node2_timeout node2_proxy >/dev/null 2>&1 || true
$TP toxic remove -n node3_timeout node3_proxy >/dev/null 2>&1 || true

$TP toxic add -n node2_timeout -t timeout -a timeout=0 node2_proxy || true
$TP toxic add -n node3_timeout -t timeout -a timeout=0 node3_proxy || true

$TP inspect node2_proxy || true
$TP inspect node3_proxy || true
sleep 8

banner "STEP 7: heal partition and restart crashed nodes"
$TP toxic remove -n node2_timeout node2_proxy >/dev/null 2>&1 || true
$TP toxic remove -n node3_timeout node3_proxy >/dev/null 2>&1 || true
$TP toxic remove -n node2_latency node2_proxy >/dev/null 2>&1 || true
$TP toxic remove -n node3_latency node3_proxy >/dev/null 2>&1 || true

docker compose up -d "$LEADER" "$SECOND"
sleep 10

banner "STEP 8: stop client"
docker compose stop client >/dev/null 2>&1 || true
docker compose rm -f client >/dev/null 2>&1 || true

banner "STEP 9: verify ledger convergence"
for n in node1 node2 node3 node4 node5; do
  echo -n "$n committed entries: "
  docker compose exec -T $n sh -c "[ -f /data/$n.log ] && wc -l < /data/$n.log || echo 0"
done

banner "STEP 10: recent Paxos evidence"
docker compose logs --since=15m node1 node2 node3 node4 node5 \
| grep -E "ELECTED LEADER|PAXOS COMMIT|PAXOS LEARN/DECIDE|LEDGER append" \
| tail -80

echo ""
echo "Fixed chaos run complete."
