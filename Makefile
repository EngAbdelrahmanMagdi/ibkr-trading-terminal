# Developer entry points for the local environment.
# Requires: GNU Make, bash, Docker with Compose v2.17.0+, curl (for `make verify`).
# All tooling (linters, secret scanning) runs in pinned containers; no local SDKs are needed.

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

COMPOSE := docker compose
COMPOSE_OBS := $(COMPOSE) --profile observability
WAIT_TIMEOUT := 240

# Host path of the repository for bind mounts (Git Bash on Windows needs a Windows-style path).
HOST_PWD := $(shell pwd -W 2>/dev/null || pwd)
# Prevent Git Bash from rewriting container paths passed to docker.
DOCKER_RUN := MSYS_NO_PATHCONV=1 docker run --rm

SHELLCHECK_IMAGE := koalaman/shellcheck:v0.11.0
GITLEAKS_IMAGE := zricethezav/gitleaks:v8.30.1
SHELL_SCRIPTS := infrastructure/scripts/bootstrap.sh infrastructure/scripts/verify-infra.sh \
                 infrastructure/postgres/init/10-create-roles.sh infrastructure/redis/start-redis.sh

.PHONY: help bootstrap up up-obs down ps logs verify test lint security clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

bootstrap: ## Check prerequisites; create .env and generate local secrets (never overwrites existing ones)
	@bash infrastructure/scripts/bootstrap.sh

up: bootstrap ## Start core infrastructure: PostgreSQL, Redis, Kafka
	$(COMPOSE) up -d --wait --wait-timeout $(WAIT_TIMEOUT)

up-obs: bootstrap ## Start core infrastructure plus Prometheus, Grafana, Tempo and the OpenTelemetry Collector
	$(COMPOSE_OBS) up -d --wait --wait-timeout $(WAIT_TIMEOUT)

down: ## Stop and remove containers (data volumes are kept)
	$(COMPOSE_OBS) down

ps: ## Show container status
	$(COMPOSE_OBS) ps

logs: ## Follow logs (all services, or SERVICE=<name>)
	$(COMPOSE_OBS) logs -f --tail=200 $(SERVICE)

verify: ## Verify connectivity, security posture and behavior (VERIFY_RESTARTS=1 adds restart checks)
	@bash infrastructure/scripts/verify-infra.sh

test: verify ## Run all available checks (currently: infrastructure verification)

lint: bootstrap ## Validate the Compose configuration and lint shell scripts (shellcheck)
	@echo "compose: default profile"; $(COMPOSE) config --quiet
	@echo "compose: observability profile"; $(COMPOSE_OBS) config --quiet
	@echo "shellcheck: $(SHELL_SCRIPTS)"
	@$(DOCKER_RUN) -v "$(HOST_PWD):/mnt:ro" -w /mnt $(SHELLCHECK_IMAGE) --severity=style $(SHELL_SCRIPTS)
	@echo "lint: ok"

security: ## Scan for secrets: git history and every file that would be committed (tracked + untracked, not ignored)
	@echo "gitleaks: git history"
	@$(DOCKER_RUN) -v "$(HOST_PWD):/repo:ro" --entrypoint sh $(GITLEAKS_IMAGE) -c \
		'git config --global --add safe.directory /repo && gitleaks git /repo --redact --no-banner'
	@echo "gitleaks: files that would be committed"
	@git ls-files -z --cached --others --exclude-standard | tar --null -T - -cf - | \
		$(DOCKER_RUN) -i --entrypoint sh $(GITLEAKS_IMAGE) -c \
		'mkdir -p /scan && tar -xf - -C /scan && gitleaks dir /scan --redact --no-banner'
	@echo "security: ok"

clean: ## Stop containers and DELETE all local data volumes (asks for confirmation; CONFIRM=yes skips it)
	@if [[ "$${CONFIRM:-}" != "yes" ]]; then \
		read -r -p "This deletes all local data volumes. Type 'yes' to continue: " answer; \
		[[ "$$answer" == "yes" ]] || { echo "aborted"; exit 1; }; \
	fi
	$(COMPOSE_OBS) down --volumes --remove-orphans
	@echo "clean: containers and volumes removed (.env and ./secrets are kept; delete them manually to regenerate)"
