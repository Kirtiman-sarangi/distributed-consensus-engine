"""
adversary.py
============
Purpose
-------
A Byzantine ("malicious") node. It subclasses ConsensusNode and overrides a
few protocol hooks to *intentionally break* PBFT, so we can demonstrate that
an honest 2f+1 quorum still reaches correct consensus and that signature
checks catch tampering. This directly satisfies Task 4 (Byzantine Adversary).

Configured attacks (env var ADVERSARY_MODE)
-------------------------------------------
  equivocate : Send CONFLICTING values to different peers during PREPARE
               (classic equivocation / double-speak).
  suppress   : Silently drop its COMMIT messages (withholding attack).
  forge      : Attempt to spoof a message as if it came from another node
               (will fail signature verification at honest peers).
  tamper     : Send a PREPARE whose payload does not match its signature
               (will be rejected by _verify_msg at honest peers).

Why it matters
--------------
PBFT tolerates f = floor((N-1)/3) Byzantine nodes. With N=5, f=1, the honest
quorum is 2f+1 = 3. The adversary cannot manufacture a conflicting quorum
because it cannot forge honest nodes' signatures, and honest nodes only count
PREPARE/COMMIT messages whose signatures verify. The malicious node is thus
"caught/ignored", exactly as the report must show.

How it connects
---------------
Started as a 6th container in docker-compose with ADVERSARY=1. It participates
in leader election like any node but misbehaves once consensus runs.
"""

from __future__ import annotations

import asyncio
import os

from node import ConsensusNode, send, PEERS, log

ADVERSARY_MODE = os.environ.get("ADVERSARY_MODE", "equivocate").lower()


class ByzantineNode(ConsensusNode):
    def __init__(self):
        super().__init__()
        self.attack = ADVERSARY_MODE
        log(f"!! BYZANTINE adversary online | attack={self.attack}")

    # Override PBFT PREPARE handling to equivocate / tamper.
    async def on_pre_prepare(self, msg):
        if not self._verify_msg(msg):
            return
        seq = msg["seq"]

        if self.attack == "suppress":
            # Accept silently but never help the protocol make progress.
            log(f"[ATTACK suppress] dropping participation for seq={seq}")
            return

        if self.attack == "equivocate":
            # Send DIFFERENT forged values to different halves of the cluster.
            log(f"[ATTACK equivocate] sending conflicting PREPAREs for seq={seq}")
            honest_value = msg["value"]
            evil_value = {"op": "DOUBLE_SPEND", "amount": 999999}
            half = len(self.peers) // 2
            for i, p in enumerate(self.peers):
                if p == self.id:
                    continue
                v = honest_value if i < half else evil_value
                out = self._sign_msg("PBFT_PREPARE", {"view": msg["view"], "seq": seq, "value": v})
                await send(p, out)
            return

        if self.attack == "tamper":
            # Sign one payload but ship a different one -> signature mismatch.
            log(f"[ATTACK tamper] shipping payload that won't match signature seq={seq}")
            out = self._sign_msg("PBFT_PREPARE", {"view": msg["view"], "seq": seq, "value": msg["value"]})
            out["value"] = {"op": "TAMPERED", "amount": -1}  # mutate AFTER signing
            await self.broadcast(out)
            return

        if self.attack == "forge":
            # Pretend to be another node (we lack its private key -> rejected).
            victim = next(p for p in self.peers if p != self.id)
            log(f"[ATTACK forge] impersonating {victim} for seq={seq}")
            out = self._sign_msg("PBFT_PREPARE", {"view": msg["view"], "seq": seq, "value": msg["value"]})
            out["from"] = victim  # claim a different identity
            await self.broadcast(out)
            return

        # default: behave
        await super().on_pre_prepare(msg)

    async def on_pbft_prepare(self, msg):
        if self.attack == "suppress":
            return
        await super().on_pbft_prepare(msg)

    async def on_commit(self, msg):
        if self.attack == "suppress":
            log(f"[ATTACK suppress] withholding COMMIT for seq={msg.get('seq')}")
            return
        await super().on_commit(msg)


if __name__ == "__main__":
    asyncio.run(ByzantineNode().run())
