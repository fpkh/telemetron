#!/usr/bin/env bash
#
# Telemetron — run the whole project with one command.
#
#   ./run.sh            full run: infra -> topics -> dashboard + generator + Flink job
#   ./run.sh --setup    setup only (infra, jar, topics, venv), no run
#
# The live dashboard starts automatically and opens in your browser. Disable it
# with DASHBOARD=0 ./run.sh  (port via DASHBOARD_PORT, default 8088).
#
# Stop the demo: Ctrl+C (generator, dashboard and job stop; containers keep running).
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
fi
# shellcheck disable=SC1091
source .venv/bin/activate
# Install deps if the heavy one (PyFlink) is missing — covers both a fresh venv
# and a half-built one. apache-flink pulls apache-beam, whose source build needs
# pkg_resources, removed in setuptools >=81; pin the build's setuptools below it.
if ! python -c 'import pyflink' >/dev/null 2>&1; then
  info "installing Python dependencies (first run takes a few minutes)..."
  pip install -q -U pip
  BUILD_CONSTRAINTS="$(mktemp)"; printf 'setuptools<81\nwheel\n' > "$BUILD_CONSTRAINTS"
  PIP_CONSTRAINT="$BUILD_CONSTRAINTS" \
    pip install -q -r generator/requirements.txt -r flink/requirements.txt
  rm -f "$BUILD_CONSTRAINTS"
  info "dependencies installed"
else
  info "dependencies already present"
fi

# Java for PyFlink's embedded mini-cluster. Flink 2.0 runs on Java 11/17; a newer
# default JDK (e.g. 21+) will crash the job. Auto-pick a compatible JDK on macOS.
if [[ "$(uname)" == "Darwin" ]] && command -v /usr/libexec/java_home >/dev/null 2>&1; then
  if [[ -z "${JAVA_HOME:-}" ]] || ! "${JAVA_HOME}/bin/java" -version 2>&1 | grep -qE '"(11|17)[.\"]'; then
    for v in 17 11; do
      if JH="$(/usr/libexec/java_home -v "$v" 2>/dev/null)"; then
        export JAVA_HOME="$JH"; info "using Java $v at $JAVA_HOME"; break
      fi
    done
  fi
fi
[[ -n "${JAVA_HOME:-}" ]] || info "warning: JAVA_HOME not set; the Flink job needs Java 11/17"

if [[ $SETUP_ONLY -eq 1 ]]; then
  step "Done. Setup complete."
  info "Run the demo: ./run.sh   (or 'make gen' / 'make job' in separate terminals)"
  exit 0
fi

# 6. Dashboard (background) + generator (background) + Flink job (foreground)
GEN_PID=""
DASH_PID=""
cleanup() {
  printf "\n\033[1;36m==> Shutting down (generator, dashboard)\033[0m\n"
  [[ -n "$GEN_PID"  ]] && kill "$GEN_PID"  2>/dev/null || true
  [[ -n "$DASH_PID" ]] && kill "$DASH_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  info "containers are still up — stop them with: docker compose down"
}
trap cleanup EXIT INT TERM

if [[ "${DASHBOARD:-1}" != "0" ]]; then
  step "Starting the live dashboard (background)"
  DASH_PORT="${DASHBOARD_PORT:-8088}"
  DASH_URL="http://localhost:${DASH_PORT}"
  DASH_LOG="$(pwd)/.dashboard.log"
  python dashboard/server.py >"$DASH_LOG" 2>&1 &
  DASH_PID=$!
  info "dashboard PID=$DASH_PID  (log: $DASH_LOG)"
  printf "    \033[1;32m\xe2\x97\x8f Dashboard is LIVE: %s\033[0m  (opening in your browser)\n" "$DASH_URL"
  info "it stays empty until the first window closes, then updates every 3s."
  ( sleep 1
    if   command -v open     >/dev/null 2>&1; then open "$DASH_URL"
    elif command -v xdg-open >/dev/null 2>&1; then xdg-open "$DASH_URL"
    fi ) >/dev/null 2>&1 &
fi

step "Starting the event generator (background)"
python generator/agent_generator.py &
GEN_PID=$!
info "generator PID=$GEN_PID"

step "Starting the Flink job (Web UI: http://localhost:${FLINK_REST_PORT:-8081})"
info "Windows close on watermark — first aggregates appear after ~1-2 minutes."
info "Dashboard: ${DASH_URL:-disabled (DASHBOARD=0)}   |   raw output: make consume"
python flink/job.py
