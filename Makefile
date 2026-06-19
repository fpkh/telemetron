# Telemetron — orchestration. All commands read variables from .env (if present).
SHELL := /bin/bash
ifneq (,$(wildcard .env))
  include .env
  export
endif

.PHONY: help run setup test up down topics seed jars venv gen job consume dashboard logs clean

help:
	@echo "make run      - run the whole project with one command (./run.sh)"
	@echo "make setup    - prepare the environment only, no run"
	@echo "make test     - aggregation unit tests"
	@echo "make up       - start Kafka + Postgres (docker compose)"
	@echo "make topics   - create the agent_events / agent_metrics topics"
	@echo "make seed     - (re)seed the agent_types dimension in Postgres"
	@echo "make jars     - download the Flink Kafka connector"
	@echo "make venv     - create .venv and install generator + job deps"
	@echo "make gen      - run the event generator"
	@echo "make job      - run the Flink job (PyFlink)"
	@echo "make consume  - read the agent_metrics output topic"
	@echo "make dashboard- web dashboard at http://localhost:8088"
	@echo "make down     - stop the infrastructure"
	@echo "make clean    - stop and remove volumes"

run:
	./run.sh

setup:
	./run.sh --setup

test:
	python3 tests/test_metrics.py

up:
	docker compose up -d
	@echo "waiting for Kafka/Postgres to be ready..."
	docker compose ps

down:
	docker compose down

clean:
	docker compose down -v

topics:
	bash scripts/create_topics.sh

seed:
	bash scripts/seed_pg.sh

jars:
	bash scripts/download_jars.sh

venv:
	python3 -m venv .venv
	. .venv/bin/activate && pip install -U pip \
	  && pip install -r generator/requirements.txt \
	  && pip install -r flink/requirements.txt

gen:
	. .venv/bin/activate && python generator/agent_generator.py

job:
	. .venv/bin/activate && python flink/job.py

consume:
	bash scripts/consume_output.sh

dashboard:
	. .venv/bin/activate && python dashboard/server.py

logs:
	docker compose logs -f
