# Distributed Consensus Engine — Paxos & PBFT (IIT Jodhpur FDS Assignment 1, Q1)

A 5-node append-only transaction ledger that keeps consensus under **crash faults**
(Mode A: Raft-style leader election + Basic Paxos, tolerates *f*=2 crashes) and
**Byzantine faults** (Mode B: PBFT with RSA signatures, tolerates *f*=1 malicious node).
Orchestrated with Docker Compose; faults injected with Toxiproxy.

```
distributed-consensus-engine/
├── src/
│   ├── node.py          # daemon: leader election + Paxos + PBFT + disk ledger
│   ├── adversary.py     # Byzantine node (subclass of node)
│   ├── client.py        # concurrent transaction generator
│   └── crypto_utils.py  # RSA keygen + sign/verify (PBFT)
├── tests/
│   └── chaos_test.sh    # Toxiproxy/crash fault injection during load
├── Dockerfile
├── docker-compose.yml   # 5 nodes + adversary + client + toxiproxy
├── requirements.txt
├── project_report.pdf   # = "Assignment (Q1) [Roll No.].pdf"
└── README.md
```

> Replace `[Roll No.]` everywhere (this README, the report filename, compose) with your roll number.

---

## 0. Prerequisites (macOS)

```bash
# Docker Desktop must be running
docker --version
docker compose version

# (optional) local syntax check without Docker
python3 -m pip install -r requirements.txt
python3 -m py_compile src/*.py && echo OK
```

## 1. Create the project & files
The repo is already structured as above. If recreating from scratch on macOS:
```bash
mkdir -p distributed-consensus-engine/src distributed-consensus-engine/tests
cd distributed-consensus-engine
# ...add the files from this repo...
chmod +x tests/chaos_test.sh
```

## 2. Build images
```bash
docker compose build
```

## 3. Run — Mode A (Paxos, crash-fault tolerance)
```bash
CONSENSUS_MODE=paxos docker compose up -d node1 node2 node3 node4 node5 toxiproxy
docker compose logs -f node1 node2 node3        # watch election + commits
```
Run the client load:
```bash
CONSENSUS_MODE=paxos docker compose up client
```

## 4. Run — Mode B (PBFT, Byzantine-fault tolerance) WITH the adversary
```bash
CONSENSUS_MODE=pbft ADVERSARY_MODE=equivocate \
  docker compose --profile byzantine up -d \
  node1 node2 node3 node4 node5 adversary toxiproxy
CONSENSUS_MODE=pbft docker compose up client
```
Switch attack: `ADVERSARY_MODE` ∈ `equivocate | suppress | tamper | forge`.

## 5. Inject faults
```bash
# Crash a node
docker compose kill node1

# Restart it
docker compose up -d --no-recreate node1

# Network latency / partition via Toxiproxy
docker exec toxiproxy /toxiproxy-cli create node2_proxy --listen 0.0.0.0:19002 --upstream node2:9000
docker exec toxiproxy /toxiproxy-cli toxic add node2_proxy -t latency -a latency=800   # 800ms latency
docker exec toxiproxy /toxiproxy-cli toxic add node2_proxy -t timeout -a timeout=0     # partition
docker exec toxiproxy /toxiproxy-cli toxic remove node2_proxy -n node2_proxy_timeout   # heal

# Full automated chaos run
./tests/chaos_test.sh
```

## 6. View logs / verify commits
```bash
docker compose logs node3 | grep -E "ELECTED|COMMIT|SIGNATURE"
docker compose exec node1 sh -c 'cat /data/node1.log'
for n in node1 node2 node3 node4 node5; do
  echo -n "$n: "; docker compose exec -T $n sh -c "wc -l < /data/$n.log"
done
```

## 7. Tear down
```bash
docker compose --profile byzantine down -v
```

---

## Testing & verification matrix

| Test | How | Expected success criterion |
|------|-----|----------------------------|
| Normal transaction flow | `docker compose up client` (paxos) | Each txn → `PAXOS COMMIT slot=…`; all nodes' ledgers grow |
| Paxos leader election | Start cluster | Exactly one `ELECTED LEADER for term N`; no two leaders same term |
| Leader failure recovery | `docker compose kill <leader>` | New `ELECTED LEADER` within ~3s; commits resume |
| Two-node crash tolerance | kill leader + one more | 3 survivors still log `PAXOS COMMIT` (majority intact) |
| PBFT normal commit | pbft mode, no attack | `PBFT COMMIT seq=…` after 2f+1 COMMITs |
| Byzantine detection | `ADVERSARY_MODE=equivocate` | No `DOUBLE_SPEND` ever committed; honest value commits |
| Signature rejection | `ADVERSARY_MODE=tamper` / `forge` | `!! SIGNATURE REJECTED` in logs; bad msg dropped |
| Partition recovery | toxiproxy timeout then remove | Commits stall on minority side, resume after heal; ledgers converge |

---

## Submission checklist (Q1)

- [ ] Git repo named `distributed-consensus-engine` with the exact structure above.
- [ ] `git init && git add -A && git commit -m "Assignment 1 Q1"` and push to your repo.
- [ ] Report exported as **`Assignment (Q1) [Roll No.].pdf`** (typed, not handwritten) — replace `[Roll No.]`.
- [ ] Report includes: architecture, key distribution, **4 chaos-evaluation log screenshots**, and the **public video link** (≤5 min: build + normal txn + Byzantine recovery).
- [ ] `docker compose build` succeeds clean.
- [ ] Both modes demonstrated (paxos & pbft) and at least one adversary attack shown caught.
- [ ] Ledgers (`/data/*.log`) show converged committed prefixes.

### Common mistakes to avoid
- Forgetting to replace `[Roll No.]` in the PDF filename (auto-grader may reject).
- Submitting a handwritten Q1 report (explicitly prohibited).
- Omitting the screenshots or the video link.
- Running PBFT mode without the `--profile byzantine` adversary (then there's nothing to "catch").
- Leaving `CONSENSUS_MODE` unset when you meant PBFT (defaults to paxos).
```
```
