"""
node.py
=======
Purpose
-------
The main consensus daemon. One instance runs per container. A single binary
implements three cooperating subsystems:

  1. Leader Election  - Raft-style heartbeats + randomized election timeouts,
                        producing a single stable leader with NO split brain
                        (majority-vote requirement).
  2. Mode A  (Paxos)  - Basic (single-decree, repeated per slot) Paxos. The
                        leader is the Proposer; all nodes are Acceptors/Learners.
                        Phases: PREPARE -> PROMISE -> ACCEPT -> ACCEPTED.
                        Tolerates up to f=2 crashes out of 5 (needs majority=3).
  3. Mode B  (PBFT)   - Practical Byzantine Fault Tolerance. The leader is the
                        Primary. Phases: PRE-PREPARE -> PREPARE -> COMMIT.
                        Tolerates f=1 Byzantine node out of 5 (needs 2f+1=3
                        matching votes). All messages are RSA-signed.

The active protocol is chosen by the CONSENSUS_MODE env var ("paxos" | "pbft").

Communication model
--------------------
Asyncio TCP. Every message is a UTF-8 JSON object, newline-delimited. A node
opens a fresh connection per send (simple and robust against partner restarts);
peers are addressed by Docker-Compose service DNS names (node1..node5).

Durability
----------
A transaction is appended to /data/<node_id>.log ONLY after the node learns it
is committed (Paxos: majority ACCEPTED; PBFT: 2f+1 COMMIT). The log is an
append-only, fsync-ed ledger -> linearizable, replayable history.

How it connects to the rest of the system
------------------------------------------
- crypto_utils.KeyStore provides sign/verify for the PBFT path.
- adversary.py subclasses ConsensusNode and overrides hook methods to inject
  Byzantine behaviour.
- client.py connects to the leader's client port and submits transactions.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from pathlib import Path
from typing import Dict, List, Optional

from crypto_utils import KeyStore

# ----------------------------------------------------------------- config
NODE_ID = os.environ.get("NODE_ID", "node1")
PEERS = [p for p in os.environ.get("PEERS", "node1,node2,node3,node4,node5").split(",") if p]
PEER_PORT = int(os.environ.get("PEER_PORT", "9000"))     # node-to-node port
CLIENT_PORT = int(os.environ.get("CLIENT_PORT", "8000"))  # client-facing port
MODE = os.environ.get("CONSENSUS_MODE", "paxos").lower()  # "paxos" | "pbft"
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))

HEARTBEAT_INTERVAL = 0.5     # leader -> followers
ELECTION_TIMEOUT_MIN = 1.5   # randomized to avoid simultaneous candidacies
ELECTION_TIMEOUT_MAX = 3.0

N = len(PEERS)
F_CRASH = (N - 1) // 2        # Paxos majority tolerance (2 for N=5)
F_BYZ = (N - 1) // 3          # PBFT Byzantine tolerance  (1 for N=5)
PAXOS_MAJORITY = N // 2 + 1   # 3 for N=5
PBFT_QUORUM = 2 * F_BYZ + 1   # 3 for N=5


def log(*args):
    print(f"[{time.strftime('%H:%M:%S')}] [{NODE_ID}]", *args, flush=True)


# =====================================================================
# Networking helper
# =====================================================================
async def send(peer: str, message: dict, port: int = PEER_PORT, timeout: float = 1.0) -> None:
    """Fire-and-forget JSON send. Failures (crashed/partitioned peer) are
    swallowed - that's the whole point of a fault-tolerant protocol."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(peer, port), timeout=timeout
        )
        writer.write((json.dumps(message) + "\n").encode())
        await writer.drain()
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass


# =====================================================================
# Main node
# =====================================================================
class ConsensusNode:
    def __init__(self):
        self.id = NODE_ID
        self.peers = PEERS
        self.mode = MODE
        self.keystore = KeyStore(self.id)

        # --- leader election (Raft-style) state ---
        self.term = 0
        self.voted_for: Optional[str] = None
        self.leader: Optional[str] = None
        self.role = "follower"            # follower | candidate | leader
        self.votes_received = set()
        self.last_heartbeat = time.time()
        self.election_deadline = self._new_election_deadline()

        # --- Paxos state (per log slot) ---
        self.paxos_slot = 0
        self.proposal_counter = 0
        self.promises: Dict[int, set] = {}
        self.accepts: Dict[int, set] = {}
        self.acceptor_state: Dict[int, dict] = {}   # slot -> {promised_n, accepted_n, accepted_val}

        # --- PBFT state (per sequence number) ---
        self.pbft_seq = 0
        self.prepares: Dict[int, set] = {}
        self.commits: Dict[int, set] = {}
        self.pbft_preprepared: Dict[int, dict] = {}

        # --- committed ledger ---
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.ledger_path = DATA_DIR / f"{self.id}.log"
        self.committed_seqs = set()
        self._load_existing_ledger()

    # ----------------------------------------------------------------
    def _new_election_deadline(self) -> float:
        return time.time() + random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def is_leader(self) -> bool:
        return self.role == "leader"

    # ================================================================
    # Server: accept inbound node-to-node messages
    # ================================================================
    async def _peer_server(self):
        server = await asyncio.start_server(self._handle_peer, "0.0.0.0", PEER_PORT)
        async with server:
            await server.serve_forever()

    async def _handle_peer(self, reader, writer):
        try:
            data = await reader.readline()
            if not data:
                return
            msg = json.loads(data.decode())
            await self.dispatch(msg)
        except Exception as e:
            log("peer handler error:", e)
        finally:
            writer.close()

    async def dispatch(self, msg: dict):
        t = msg.get("type")
        handlers = {
            # leader election
            "REQUEST_VOTE": self.on_request_vote,
            "VOTE": self.on_vote,
            "HEARTBEAT": self.on_heartbeat,
            # paxos
            "PREPARE": self.on_prepare,
            "PROMISE": self.on_promise,
            "ACCEPT": self.on_accept,
            "ACCEPTED": self.on_accepted,
            "DECIDE": self.on_decide,
            "SYNC_REQUEST": self.on_sync_request,
            "SYNC_RESPONSE": self.on_sync_response,
            # pbft
            "PRE_PREPARE": self.on_pre_prepare,
            "PBFT_PREPARE": self.on_pbft_prepare,
            "COMMIT": self.on_commit,
        }
        handler = handlers.get(t)
        if handler:
            await handler(msg)

    async def broadcast(self, message: dict):
        await asyncio.gather(*[
            send(p, message) for p in self.peers if p != self.id
        ])

    # ================================================================
    # LEADER ELECTION  (Raft-style, majority vote => no split brain)
    # ================================================================
    async def election_loop(self):
        while True:
            await asyncio.sleep(0.2)
            now = time.time()
            if self.role == "leader":
                continue
            if now >= self.election_deadline:
                await self.start_election()

    async def start_election(self):
        self.term += 1
        self.role = "candidate"
        self.voted_for = self.id
        self.votes_received = {self.id}
        self.election_deadline = self._new_election_deadline()
        log(f"starting election for term {self.term}")
        await self.broadcast({
            "type": "REQUEST_VOTE", "term": self.term, "candidate": self.id,
        })

    async def on_request_vote(self, msg):
        term, candidate = msg["term"], msg["candidate"]
        if term > self.term:
            self.term = term
            self.voted_for = None
            self.role = "follower"
        grant = term >= self.term and self.voted_for in (None, candidate)
        if grant:
            self.voted_for = candidate
            self.election_deadline = self._new_election_deadline()
        await send(candidate, {
            "type": "VOTE", "term": self.term, "voter": self.id, "granted": grant,
        })

    async def on_vote(self, msg):
        if self.role != "candidate" or msg["term"] != self.term:
            return
        if msg.get("granted"):
            self.votes_received.add(msg["voter"])
            if len(self.votes_received) >= PAXOS_MAJORITY:
                self.role = "leader"
                self.leader = self.id
                log(f"ELECTED LEADER for term {self.term} "
                    f"({len(self.votes_received)}/{N} votes)")
                asyncio.create_task(self.heartbeat_loop())

    async def heartbeat_loop(self):
        while self.role == "leader":
            await self.broadcast({
                "type": "HEARTBEAT", "term": self.term, "leader": self.id,
            })
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def on_heartbeat(self, msg):
        if msg["term"] >= self.term:
            self.term = msg["term"]
            self.leader = msg["leader"]
            self.role = "follower"
            self.voted_for = None
            self.last_heartbeat = time.time()
            self.election_deadline = self._new_election_deadline()

    # ================================================================
    # MODE A : BASIC PAXOS  (leader = proposer)
    #   PREPARE -> PROMISE -> ACCEPT -> ACCEPTED
    # ================================================================
    async def paxos_propose(self, value: dict):
        if not self.is_leader():
            log("ignoring proposal - not leader")
            return
        slot = self.paxos_slot
        self.proposal_counter += 1
        # globally-ordered proposal number: (counter, node-id) avoids ties
        n = self.proposal_counter * 100 + self._numeric_id()
        self.promises[slot] = set()
        self.accepts[slot] = set()
        self._pending_value = value
        log(f"PAXOS phase-1 PREPARE slot={slot} n={n} value={value['op']}")
        # count self
        self.promises[slot].add(self.id)
        await self.broadcast({
            "type": "PREPARE", "slot": slot, "n": n, "from": self.id,
        })

    def _numeric_id(self) -> int:
        return int("".join(c for c in self.id if c.isdigit()) or "0")

    async def on_prepare(self, msg):
        slot, n = msg["slot"], msg["n"]
        st = self.acceptor_state.setdefault(slot, {"promised_n": -1, "accepted_n": -1, "accepted_val": None})
        if n > st["promised_n"]:
            st["promised_n"] = n
            await send(msg["from"], {
                "type": "PROMISE", "slot": slot, "n": n, "voter": self.id,
                "accepted_n": st["accepted_n"], "accepted_val": st["accepted_val"],
            })

    async def on_promise(self, msg):
        slot, n = msg["slot"], msg["n"]
        if slot not in self.promises:
            return
        self.promises[slot].add(msg["voter"])
        # Paxos correctness: if any acceptor already accepted a value, adopt
        # the one with the highest accepted_n.
        if msg.get("accepted_val") is not None:
            prev = getattr(self, "_highest_accept", {}).get(slot, (-1, None))
            if msg["accepted_n"] > prev[0]:
                self._highest_accept = getattr(self, "_highest_accept", {})
                self._highest_accept[slot] = (msg["accepted_n"], msg["accepted_val"])
        if len(self.promises[slot]) == PAXOS_MAJORITY:
            value = getattr(self, "_highest_accept", {}).get(slot, (None, None))[1] or self._pending_value
            log(f"PAXOS phase-2 ACCEPT slot={slot} n={n} (majority promised)")
            self.accepts[slot].add(self.id)
            await self.broadcast({
                "type": "ACCEPT", "slot": slot, "n": n, "value": value, "from": self.id,
            })

    async def on_accept(self, msg):
        slot, n = msg["slot"], msg["n"]
        st = self.acceptor_state.setdefault(slot, {"promised_n": -1, "accepted_n": -1, "accepted_val": None})
        if n >= st["promised_n"]:
            st["promised_n"] = n
            st["accepted_n"] = n
            st["accepted_val"] = msg["value"]
            await send(msg["from"], {
                "type": "ACCEPTED", "slot": slot, "n": n, "value": msg["value"], "voter": self.id,
            })

    async def on_accepted(self, msg):
        slot = msg["slot"]
        s = self.accepts.setdefault(slot, set())
        s.add(msg["voter"])
        if len(s) == PAXOS_MAJORITY:
            log(f"PAXOS COMMIT slot={slot} value={msg['value']['op']} "
                f"(majority {PAXOS_MAJORITY}/{N})")
            self._commit_to_disk(slot, msg["value"], protocol="paxos")
            await self.broadcast({
                "type": "DECIDE",
                "slot": slot,
                "value": msg["value"],
                "from": self.id,
            })
            self.paxos_slot = max(self.paxos_slot, slot + 1)

    async def on_decide(self, msg):
        slot = msg["slot"]
        if slot not in self.committed_seqs:
            log(f"PAXOS LEARN/DECIDE slot={slot} value={msg['value']['op']}")
            self._commit_to_disk(slot, msg["value"], protocol="paxos")
            self.paxos_slot = max(self.paxos_slot, slot + 1)

    # ================================================================
    # MODE B : PBFT  (leader = primary)
    #   PRE-PREPARE -> PREPARE -> COMMIT   (all messages signed)
    # ================================================================
    async def pbft_request(self, value: dict):
        if not self.is_leader():
            log("ignoring PBFT request - not primary")
            return
        seq = self.pbft_seq
        self.pbft_seq += 1
        payload = {"view": self.term, "seq": seq, "value": value}
        signed = self._sign_msg("PRE_PREPARE", payload)
        log(f"PBFT PRE-PREPARE seq={seq} value={value['op']}")
        self.pbft_preprepared[seq] = value
        self.prepares.setdefault(seq, set()).add(self.id)
        await self.broadcast(signed)

    def _sign_msg(self, mtype: str, payload: dict) -> dict:
        body = dict(payload)
        body["type"] = mtype
        body["from"] = self.id
        body["sig"] = self.keystore.sign(payload)
        return body

    def _verify_msg(self, msg: dict) -> bool:
        payload = {k: msg[k] for k in ("view", "seq", "value") if k in msg}
        ok = self.keystore.verify(msg["from"], payload, msg.get("sig", ""))
        if not ok:
            log(f"!! SIGNATURE REJECTED from {msg.get('from')} "
                f"seq={msg.get('seq')} (dropping spoofed/tampered msg)")
        return ok

    async def on_pre_prepare(self, msg):
        if not self._verify_msg(msg):
            return
        # Only accept pre-prepare from the current primary (the leader).
        if msg["from"] != self.leader:
            log(f"!! PRE-PREPARE from non-primary {msg['from']} ignored")
            return
        seq = msg["seq"]
        self.pbft_preprepared[seq] = msg["value"]
        out = self._sign_msg("PBFT_PREPARE", {"view": msg["view"], "seq": seq, "value": msg["value"]})
        self.prepares.setdefault(seq, set()).add(self.id)
        await self.broadcast(out)

    async def on_pbft_prepare(self, msg):
        if not self._verify_msg(msg):
            return
        seq = msg["seq"]
        s = self.prepares.setdefault(seq, set())
        s.add(msg["from"])
        # "prepared" certificate => 2f+1 matching PREPAREs (incl. pre-prepare)
        if len(s) >= PBFT_QUORUM and seq not in self.commits:
            self.commits[seq] = {self.id}
            out = self._sign_msg("COMMIT", {"view": msg["view"], "seq": seq, "value": msg["value"]})
            log(f"PBFT PREPARED seq={seq} -> broadcasting COMMIT")
            await self.broadcast(out)

    async def on_commit(self, msg):
        if not self._verify_msg(msg):
            return
        seq = msg["seq"]
        s = self.commits.setdefault(seq, set())
        s.add(msg["from"])
        if len(s) >= PBFT_QUORUM and seq not in self.committed_seqs:
            log(f"PBFT COMMIT seq={seq} value={msg['value']['op']} "
                f"(quorum {PBFT_QUORUM}/{N}, Byzantine-safe)")
            self._commit_to_disk(seq, msg["value"], protocol="pbft")

    # ================================================================
    # Ledger catch-up / recovery sync
    # ================================================================
    def _load_existing_ledger(self):
        if not self.ledger_path.exists():
            return
        try:
            with open(self.ledger_path) as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                        seq = int(rec["seq"])
                        self.committed_seqs.add(seq)
                        self.paxos_slot = max(self.paxos_slot, seq + 1)
                        self.pbft_seq = max(self.pbft_seq, seq + 1)
                    except Exception:
                        pass
            if self.committed_seqs:
                log(f"loaded existing ledger entries: {len(self.committed_seqs)}")
        except Exception as e:
            log("ledger load error:", e)

    def _first_missing_seq(self):
        if not self.committed_seqs:
            return 0
        max_seq = max(self.committed_seqs)
        for i in range(max_seq + 1):
            if i not in self.committed_seqs:
                return i
        return max_seq + 1

    async def sync_loop(self):
        while True:
            await asyncio.sleep(3)
            await self.request_log_sync()

    async def request_log_sync(self):
        from_seq = self._first_missing_seq()
        await self.broadcast({
            "type": "SYNC_REQUEST",
            "from": self.id,
            "from_seq": from_seq,
        })

    async def on_sync_request(self, msg):
        from_seq = int(msg.get("from_seq", 0))
        records = []
        if self.ledger_path.exists():
            with open(self.ledger_path) as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                        if int(rec.get("seq", -1)) >= from_seq:
                            records.append(rec)
                    except Exception:
                        pass

        # avoid very large TCP messages
        records = records[:500]

        await send(msg["from"], {
            "type": "SYNC_RESPONSE",
            "from": self.id,
            "records": records,
        })

    async def on_sync_response(self, msg):
        changed = 0
        records = msg.get("records", [])
        records = sorted(records, key=lambda r: int(r.get("seq", -1)))

        for rec in records:
            try:
                seq = int(rec["seq"])
                if seq not in self.committed_seqs:
                    self._commit_to_disk(
                        seq,
                        rec["value"],
                        rec.get("protocol", self.mode)
                    )
                    self.paxos_slot = max(self.paxos_slot, seq + 1)
                    self.pbft_seq = max(self.pbft_seq, seq + 1)
                    changed += 1
            except Exception:
                pass

        if changed:
            log(f"SYNC recovered {changed} missing ledger entries from {msg.get('from')}")

    # ================================================================
    # Durable ledger
    # ================================================================
    def _commit_to_disk(self, seq, value, protocol):
        if seq in self.committed_seqs:
            return
        self.committed_seqs.add(seq)
        record = {
            "seq": seq, "protocol": protocol, "value": value,
            "term": self.term, "ts": time.time(),
        }
        with open(self.ledger_path, "a") as f:
            f.write(json.dumps(record) + "\n")
            f.flush()
            os.fsync(f.fileno())
        log(f"LEDGER append seq={seq} -> {self.ledger_path}")

    # ================================================================
    # Client-facing server
    # ================================================================
    async def _client_server(self):
        server = await asyncio.start_server(self._handle_client, "0.0.0.0", CLIENT_PORT)
        async with server:
            await server.serve_forever()

    async def _handle_client(self, reader, writer):
        try:
            data = await reader.readline()
            req = json.loads(data.decode())
            if req.get("type") == "WHO_IS_LEADER":
                resp = {"leader": self.leader, "is_leader": self.is_leader()}
                writer.write((json.dumps(resp) + "\n").encode())
                await writer.drain()
                return
            value = req.get("txn", req)
            if not self.is_leader():
                writer.write((json.dumps({"status": "redirect", "leader": self.leader}) + "\n").encode())
                await writer.drain()
                return
            if self.mode == "pbft":
                await self.pbft_request(value)
            else:
                await self.paxos_propose(value)
            writer.write((json.dumps({"status": "submitted", "leader": self.id}) + "\n").encode())
            await writer.drain()
        except Exception as e:
            log("client handler error:", e)
        finally:
            writer.close()

    # ================================================================
    async def run(self):
        log(f"booting | mode={self.mode} | peers={self.peers} | "
            f"N={N} f_crash={F_CRASH} f_byz={F_BYZ}")
        self.keystore.wait_for_peers(self.peers)
        await asyncio.gather(
            self._peer_server(),
            self._client_server(),
            self.election_loop(),
            self.sync_loop(),
        )


if __name__ == "__main__":
    node = ConsensusNode()
    asyncio.run(node.run())
