"""
client.py
=========
Purpose
-------
A concurrent transaction generator. It discovers the current leader, then
submits a stream of append-only ledger transactions (transfers). If a node
replies "redirect" (it is not the leader) the client retries against the named
leader - this is how clients cope with leader changes after a crash.

How it connects
---------------
Talks to each node's CLIENT_PORT (8000). Used by chaos_test.sh and during the
demo to keep load on the cluster while faults are injected.

Expected output
---------------
  [client] submitted txn #1 -> leader node3 : transfer 50 A->B
  [client] redirect: retrying via node2
  ...
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time

NODES = ["node1", "node2", "node3", "node4", "node5"]
CLIENT_PORT = 8000


async def rpc(host: str, port: int, message: dict, timeout: float = 2.0):
    reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
    writer.write((json.dumps(message) + "\n").encode())
    await writer.drain()
    data = await asyncio.wait_for(reader.readline(), timeout)
    writer.close()
    await writer.wait_closed()
    return json.loads(data.decode())


async def find_leader():
    for host in NODES:
        try:
            r = await rpc(host, CLIENT_PORT, {"type": "WHO_IS_LEADER"})
            if r.get("leader"):
                return r["leader"]
        except Exception:
            continue
    return None


async def submit(txn: dict):
    """Submit one txn, following redirects to the leader."""
    target = await find_leader() or random.choice(NODES)
    for _ in range(len(NODES) + 2):
        try:
            r = await rpc(target, CLIENT_PORT, {"type": "TXN", "txn": txn})
            if r.get("status") == "submitted":
                return target
            if r.get("status") == "redirect" and r.get("leader"):
                target = r["leader"]
                print(f"[client] redirect: retrying via {target}", flush=True)
                continue
        except Exception:
            target = random.choice(NODES)
            await asyncio.sleep(0.3)
    return None


async def main(count: int, rate: float):
    accounts = ["A", "B", "C", "D", "E"]
    for i in range(1, count + 1):
        src, dst = random.sample(accounts, 2)
        amount = random.randint(1, 100)
        txn = {"op": "transfer", "id": i, "src": src, "dst": dst,
               "amount": amount, "ts": time.time()}
        leader = await submit(txn)
        status = f"-> leader {leader}" if leader else "-> FAILED (no leader, retrying load)"
        print(f"[client] submitted txn #{i} {status} : transfer {amount} {src}->{dst}", flush=True)
        await asyncio.sleep(1.0 / rate)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=50, help="number of transactions")
    ap.add_argument("--rate", type=float, default=2.0, help="txns per second")
    args = ap.parse_args()
    asyncio.run(main(args.count, args.rate))
