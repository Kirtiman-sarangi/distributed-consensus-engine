# Single image used by every node, the adversary and the client.
# The entrypoint command is overridden per-service in docker-compose.yml.
FROM python:3.11-slim

# tc / iproute2 lets us optionally shape traffic with Linux `tc` as an
# alternative to Toxiproxy (Fault Simulation Tooling requirement).
RUN apt-get update \
    && apt-get install -y --no-install-recommends iproute2 procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/

# Shared volumes are mounted at runtime:
#   /keys  -> public-key exchange for PBFT
#   /data  -> append-only ledgers
VOLUME ["/keys", "/data"]

CMD ["python", "node.py"]
