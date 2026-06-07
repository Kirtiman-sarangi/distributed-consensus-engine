"""
crypto_utils.py
================
Purpose
-------
Key generation, message signing and signature verification for the PBFT
(Mode B) path. PBFT requires *authenticated* messages so that a single
Byzantine node cannot forge a request on behalf of an honest peer or
equivocate without being detected. We use RSA-2048 with PSS padding over a
SHA-256 digest from the `cryptography` library (a hard requirement of the
assignment).

Key distribution model
----------------------
Each node owns a private key it never shares. On startup every node writes
its *public* key (PEM) into a shared Docker volume mounted at `/keys`
(file name `<node_id>.pub`). Because every container mounts the same
`keys` volume, each node can read every peer's public key and therefore
verify any signed message. This mirrors a real PKI bootstrap where public
keys are distributed out-of-band before the protocol starts.

How it connects to the rest of the system
------------------------------------------
`node.py` (and `adversary.py`, which subclasses it) imports `KeyStore`.
Before broadcasting any PBFT message a node calls `sign()`; on receipt it
calls `verify()` using the sender's public key. Messages that fail
verification are dropped and logged, which is exactly how PBFT neutralises
spoofing by the malicious node.
"""

from __future__ import annotations

import base64
import json
import os
import time
from pathlib import Path
from typing import Dict, Optional

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.exceptions import InvalidSignature

KEYS_DIR = Path(os.environ.get("KEYS_DIR", "/keys"))


def canonical(payload: dict) -> bytes:
    """Deterministic byte serialisation so signer and verifier hash the
    exact same bytes regardless of dict ordering."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


class KeyStore:
    """Owns this node's private key and caches peers' public keys."""

    def __init__(self, node_id: str, keys_dir: Path = KEYS_DIR):
        self.node_id = node_id
        self.keys_dir = keys_dir
        self.keys_dir.mkdir(parents=True, exist_ok=True)
        self._private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self._peer_keys: Dict[str, object] = {}
        self._publish_public_key()

    # ------------------------------------------------------------------ keys
    def _publish_public_key(self) -> None:
        pub_pem = self._private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        # Atomic write: temp file then rename, so a peer never reads a
        # half-written PEM.
        tmp = self.keys_dir / f"{self.node_id}.pub.tmp"
        final = self.keys_dir / f"{self.node_id}.pub"
        tmp.write_bytes(pub_pem)
        tmp.replace(final)

    def _load_peer_key(self, peer_id: str) -> Optional[object]:
        if peer_id in self._peer_keys:
            return self._peer_keys[peer_id]
        path = self.keys_dir / f"{peer_id}.pub"
        if not path.exists():
            return None
        key = serialization.load_pem_public_key(path.read_bytes())
        self._peer_keys[peer_id] = key
        return key

    def wait_for_peers(self, peer_ids, timeout: float = 30.0) -> None:
        """Block until every peer has published its public key (bootstrap)."""
        deadline = time.time() + timeout
        missing = set(peer_ids) - {self.node_id}
        while missing and time.time() < deadline:
            missing = {p for p in missing if not (self.keys_dir / f"{p}.pub").exists()}
            if missing:
                time.sleep(0.5)

    # ------------------------------------------------------------------ sign
    def sign(self, payload: dict) -> str:
        signature = self._private_key.sign(
            canonical(payload),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode()

    def verify(self, sender_id: str, payload: dict, signature_b64: str) -> bool:
        key = self._load_peer_key(sender_id)
        if key is None:
            return False
        try:
            key.verify(
                base64.b64decode(signature_b64),
                canonical(payload),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH,
                ),
                hashes.SHA256(),
            )
            return True
        except (InvalidSignature, ValueError, TypeError):
            return False


if __name__ == "__main__":
    # Self-test: sign and verify a message round-trip.
    ks = KeyStore("test-node", keys_dir=Path("./_keytest"))
    msg = {"op": "transfer", "amount": 10}
    sig = ks.sign(msg)
    ks._peer_keys["test-node"] = ks._private_key.public_key()
    assert ks.verify("test-node", msg, sig), "valid signature must verify"
    tampered = {"op": "transfer", "amount": 9999}
    assert not ks.verify("test-node", tampered, sig), "tampered payload must fail"
    print("crypto_utils self-test passed")
