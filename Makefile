# Developer entry points for the local environment.
# Requires: GNU Make, bash, Docker with Compose v2.17.0+, curl (for `make verify`).
# All tooling (linters, contract tests, secret scanning) runs in pinned containers; no local SDKs are needed.

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
REDOCLY_IMAGE := redocly/cli:2.54.3
ASYNCAPI_IMAGE := asyncapi/cli:6.1.0
MAVEN_IMAGE := maven:3.9.16-eclipse-temurin-25
GO_IMAGE := golang:1.27.1
PYTHON_IMAGE := python:3.14.7-slim
NODE_IMAGE := node:24.21.0

SHELL_SCRIPTS := infrastructure/scripts/bootstrap.sh infrastructure/scripts/verify-infra.sh \
                 infrastructure/postgres/init/10-create-roles.sh infrastructure/redis/start-redis.sh

# Contract tests run on a copy of the sources inside the container (repository mounted read-only).
CONTRACT_COPY := mkdir -p /work/tests && cp -r /src/contracts /work/ && cp -r /src/tests/contract /work/tests/ && cd /work/tests/contract
CONTRACT_RUN := $(DOCKER_RUN) -v "$(HOST_PWD):/src:ro"

.PHONY: help bootstrap up up-obs down ps logs verify test test-contract contract-java contract-go contract-python \
        contract-typescript lint lint-contracts security clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

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

verify: ## Verify infrastructure connectivity, security posture and behavior (VERIFY_RESTARTS=1 adds restart checks)
	@bash infrastructure/scripts/verify-infra.sh

test: test-contract verify ## Run all available checks: contract tests, then infrastructure verification

test-contract: contract-java contract-go contract-python contract-typescript ## Run the contract tests in all four languages

contract-java: ## Contract tests - Java (networknt json-schema-validator, Jackson, JUnit)
	@$(CONTRACT_RUN) -v trading-terminal-m2:/root/.m2 $(MAVEN_IMAGE) sh -c \
		'$(CONTRACT_COPY)/java && mvn -B -ntp -q test'
	@echo "contract-java: ok"

contract-go: ## Contract tests - Go (santhosh-tekuri/jsonschema)
	@$(CONTRACT_RUN) -v trading-terminal-gomod:/go/pkg/mod $(GO_IMAGE) sh -c \
		'$(CONTRACT_COPY)/go && go vet ./... && go test -count=1 ./...'
	@echo "contract-go: ok"

contract-python: ## Contract tests - Python (jsonschema) including the OpenAPI/AsyncAPI reference checks
	@$(CONTRACT_RUN) -v trading-terminal-pip:/root/.cache/pip $(PYTHON_IMAGE) sh -c \
		'$(CONTRACT_COPY)/python && pip install --quiet --disable-pip-version-check --root-user-action=ignore -r requirements.txt && python -m unittest'
	@echo "contract-python: ok"

contract-typescript: ## Contract tests - TypeScript (Ajv) with strict type checking
	@$(CONTRACT_RUN) -v trading-terminal-npm:/root/.npm $(NODE_IMAGE) sh -c \
		'$(CONTRACT_COPY)/typescript && npm ci --ignore-scripts --no-audit --no-fund --loglevel=error && npm test --silent'
	@echo "contract-typescript: ok"

lint: bootstrap lint-contracts ## Validate Compose, lint shell scripts, and lint the API contracts
	@echo "compose: default profile"; $(COMPOSE) config --quiet
	@echo "compose: observability profile"; $(COMPOSE_OBS) config --quiet
	@echo "shellcheck: $(SHELL_SCRIPTS)"
	@$(DOCKER_RUN) -v "$(HOST_PWD):/mnt:ro" -w /mnt $(SHELLCHECK_IMAGE) --severity=style $(SHELL_SCRIPTS)
	@echo "lint: ok"

lint-contracts: ## Lint the OpenAPI documents (Redocly) and validate the AsyncAPI documents
	@echo "openapi: redocly lint"
	@$(DOCKER_RUN) -v "$(HOST_PWD)/contracts:/spec:ro" -w /spec $(REDOCLY_IMAGE) lint --config redocly.yaml \
		--format=summary openapi/trading-api.yaml openapi/realtime-gateway-api.yaml
	@for spec in asyncapi/events.yaml asyncapi/market-stream.yaml; do \
		echo "asyncapi: validate $$spec"; \
		$(DOCKER_RUN) -v "$(HOST_PWD)/contracts:/spec:ro" -w /spec --entrypoint sh $(ASYNCAPI_IMAGE) -c \
			'printf "{\"analyticsEnabled\":\"false\",\"infoMessageShown\":\"true\",\"userID\":\"none\"}" > /tmp/analytics.json && \
			 ASYNCAPI_METRICS_CONFIG_PATH=/tmp/analytics.json SUPPRESS_NO_CONFIG_WARNING=1 asyncapi validate '"$$spec"' --fail-severity=warn'; \
	done

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
