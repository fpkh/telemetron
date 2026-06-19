#!/usr/bin/env bash
#
# Telemetron — run the whole project with one command.
#
#   ./run.sh            full run: infra -> topics -> generator + Flink job
#   ./run.sh --setup    setup only (infra, jar, topics, venv), no run
#
# Stop the demo: Ctrl+C (generator and job stop; containers keep running).
# Tear infra down later: docker compose down
#
set -euo pipefail
cd "$(dirname "$0")"

SETUP_ONLY=0
[[ "${1:-}" == "--setup" ]] && SETUP_ONLY=1

step() { printf "\n\033[1;36m==> %s\033[0m\n" "$*"; }
info() { printf "    %s\n" "$*"; }
die()  { printf "\n\033[1;31mError: %s\033[0m\n" "$*" >&2; exit 1; }

command -v docker >/dev/null || die "docker not found"
docker compose version >/dev/null 2>&1 || die "'docker compose' not found"

# 0. .env
if [[ ! -f .env ]]; then
  cp .env.example .env
  info "created .env from .env.example"
fi
set -a; source .env; set +a

# 1. Infrastructure
step "Starting Kafka + Postgres"
docker compose up -d

# 2. Wait for health (uses each container's own healthcheck)
wait_healthy() {
  local name="$1" tries="${2:-60}" status
  for ((i = 1; i <= tries; i++)); do
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$name" 2>/dev/null || echo missing)"
    case "$status" in
      healthy) return 0 ;;
      missing) die "container '$name' is not running (check: docker compose ps)" ;;
    esac
    sleep 3
  done
  return 1
}

step "Waiting for services to become healthy (up to ~3 min on first boot)"
if ! wait_healthy kafka 60; then
  echo; docker compose ps; echo
  docker compose logs --tail=40 kafka
  die "Kafka did not become healthy. See the logs above."
fi
info "Kafka is healthy"
if ! wait_healthy postgres 30; then
  echo; docker compose ps; echo
  docker compose logs --tail=40 postgres
  die "Postgres did not become healthy. See the logs above."
fi
info "Postgres is healthy"

# 3. Kafka connector for Flink
step "Checking the Flink Kafka connector"
bash scripts/download_jars.sh

# 4. Topics
step "Creating topics"
bash scripts/create_topics.sh

# 5. Python environment
step "Preparing the Python environment (.venv)"
if [[ ! -d .venv ]]; then
  python3 -m venv .venv
  # shellcheck disable=SC1091
  source .venv/bin/activate
  pip install -q -U pip
  pip install -q -r generator/requirements.txt -r flink/requirements.txt
  info "dependencies installed"
else
  # shellcheck disable=SC1091
  source .venv/bin/activate
  info ".venv already exists — skipping install"
fi

if [[ $SETUP_ONLY -eq 1 ]]; then
  step "Done. Setup complete."
  info "Run the demo: ./run.sh   (or 'make gen' / 'make job' in separate terminals)"
  exit 0
fi

# 6. Generator in the background + job in the foreground
step "Starting the event generator (background)"
python generator/agent_generator.py &
GEN_PID=$!
cleanup() {
  printf "\n\033[1;36m==> Stopping the generator\033[0m\n"
  kill "$GEN_PID" 2>/dev/null || true
  wait "$GEN_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM
info "generator PID=$GEN_PID"

step "Starting the Flink job (Web UI: http://localhost:${FLINK_REST_PORT:-8081})"
info "Windows close on watermark — first aggregates appear after ~1-2 minutes."
info "Watch the output here and/or run: make consume"
python flink/job.py
