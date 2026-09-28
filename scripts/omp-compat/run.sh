#!/usr/bin/env bash
# Checks that Parley loads in OMP and two OMP sessions can ask and reply through it.
# Needs OMP (npm @oh-my-pi/pi-coding-agent) and Bun >= 1.3.14 on PATH, or OMP_BIN set.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
OMP_BIN=$(command -v "${OMP_BIN:-omp}")
echo "Using $("$OMP_BIN" --version)"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/parley-omp.XXXXXX")
export HOME="$WORK/home" PI_CODING_AGENT_DIR="$WORK/home/.omp/agent" OMP_SKIP_SETUP=1
mkdir -p "$PI_CODING_AGENT_DIR" "$WORK/project"
PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
cat >"$PI_CODING_AGENT_DIR/models.yml" <<YAML
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
PORT=$PORT REQUEST_LOG="$WORK/requests.jsonl" node "$ROOT/scripts/omp-compat/fake-openai-server.mjs" >"$WORK/server.log" 2>&1 &
SERVER=$!
cleanup() {
  kill "$SERVER" 2>/dev/null || true
  # The broker belongs to this temporary home; stop it gracefully.
  [ -f "$PI_CODING_AGENT_DIR/parley/broker.pid" ] && ps -o pid=,args= -p "$(cat "$PI_CODING_AGENT_DIR/parley/broker.pid")" | cut -c1-160 | sed 's/^/broker: /'
  [ -f "$PI_CODING_AGENT_DIR/parley/broker.pid" ] && kill -TERM "$(cat "$PI_CODING_AGENT_DIR/parley/broker.pid")" 2>/dev/null || true
  echo "Artifacts: $WORK"
}
trap cleanup EXIT
for _ in $(seq 1 50); do grep -q fake-openai-ready "$WORK/server.log" 2>/dev/null && break; sleep 0.1; done
node "$ROOT/scripts/omp-compat/probe.mjs" "$OMP_BIN" "$ROOT/extension.ts" "$WORK/project" "$WORK/requests.jsonl"
