#!/usr/bin/env bash
# Checks that an OMP session can link this computer with another one over SSH
# and see its sessions. "Computers" are two home directories here, reached
# through the stand-in ssh in test/fake-ssh.mjs. Needs what run.sh needs.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
OMP_BIN=$(command -v "${OMP_BIN:-omp}")
echo "Using $("$OMP_BIN" --version)"
# Short: Unix socket paths under it must stay within 104 bytes.
WORK=$(mktemp -d /tmp/plo.XXXXXX)
LAPTOP="$WORK/laptop" BOX="$WORK/box"
mkdir -p "$LAPTOP/.omp/agent" "$BOX/.omp/agent" "$WORK/project" "$WORK/bin"
cp "$ROOT/test/fake-ssh.mjs" "$WORK/bin/ssh"
export PATH="$WORK/bin:$PATH" PI_PARLEY_TEST_SSH_HOMES="{\"box\":\"$BOX\"}" PI_PARLEY_LINK_LABEL=laptop PI_PARLEY_LINK_IDLE_MS=1000 OMP_SKIP_SETUP=1
unset PI_CODING_AGENT_DIR
PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
for home in "$LAPTOP" "$BOX"; do
cat >"$home/.omp/agent/models.yml" <<YAML
providers:
  compat:
    baseUrl: http://127.0.0.1:${PORT}/v1
    api: openai-completions
    auth: none
    models:
      - id: compat-model
        name: Parley OMP fixture
        reasoning: false
        input: [text]
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }
        contextWindow: 128000
        maxTokens: 4096
YAML
done
PORT=$PORT REQUEST_LOG="$WORK/requests.jsonl" node "$ROOT/scripts/omp-compat/fake-openai-server.mjs" >"$WORK/server.log" 2>&1 &
SERVER=$!
cleanup() {
  kill "$SERVER" 2>/dev/null || true
  # Brokers belong to these temporary homes (Parley's default agent dir there); stop them gracefully.
  for home in "$LAPTOP" "$BOX"; do
    pidfile="$home/.pi/agent/parley/broker.pid"
    [ -f "$pidfile" ] && kill -TERM "$(cat "$pidfile")" 2>/dev/null || true
  done
  echo "Artifacts: $WORK"
}
trap cleanup EXIT
for _ in $(seq 1 50); do grep -q fake-openai-ready "$WORK/server.log" 2>/dev/null && break; sleep 0.1; done
node "$ROOT/scripts/omp-compat/link-probe.mjs" "$OMP_BIN" "$ROOT/extension.ts" "$WORK/project" "$WORK/requests.jsonl" "$LAPTOP" "$BOX"
