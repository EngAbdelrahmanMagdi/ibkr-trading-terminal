# Developer entry points for the local environment.
# Requires: GNU Make, bash, Docker with Compose v2.17.0+, curl (for `make verify`).
# All tooling (linters, tests, secret scanning) runs in pinned containers; no local SDKs are needed.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

COMPOSE := docker compose
COMPOSE_OBS := $(COMPOSE) --profile observability
COMPOSE_MOCK := $(COMPOSE) --profile gateway --profile core --profile web --profile ai
IBKR_OVERRIDE := -f docker-compose.yml -f infrastructure/ibkr/compose.ibkr.yml
IBKR_FAKE_OVERRIDE := -f docker-compose.yml -f infrastructure/ibkr/compose.ibkr-fake.yml
COMPOSE_IBKR := $(COMPOSE) $(IBKR_OVERRIDE) --profile gateway
COMPOSE_IBKR_PAPER := $(COMPOSE) $(IBKR_OVERRIDE) --profile gateway --profile core
COMPOSE_IBKR_FAKE := $(COMPOSE) $(IBKR_FAKE_OVERRIDE) --profile gateway
COMPOSE_ALL := $(COMPOSE) $(IBKR_FAKE_OVERRIDE) --profile observability --profile gateway --profile core --profile web --profile ai
WAIT_TIMEOUT := 240

# Host path of the repository for bind mounts (Cygwin/Git Bash need a Windows-style path).
HOST_PWD := $(shell cygpath -m "$$(pwd)" 2>/dev/null || pwd -W 2>/dev/null || pwd)
# Prevent Git Bash from rewriting container paths passed to docker.
DOCKER_RUN := MSYS_NO_PATHCONV=1 docker run --rm

SHELLCHECK_IMAGE := koalaman/shellcheck:v0.11.0
GITLEAKS_IMAGE := zricethezav/gitleaks:v8.30.1
REDOCLY_IMAGE := redocly/cli:2.54.3
ASYNCAPI_IMAGE := asyncapi/cli:6.1.0
MAVEN_IMAGE := maven:3.9.16-eclipse-temurin-25
GO_IMAGE := golang:1.27.1
GOLANGCI_IMAGE := golangci/golangci-lint:v2.14.0
GOVULNCHECK := golang.org/x/vuln/cmd/govulncheck@v1.8.0
OSV_SCANNER_IMAGE := ghcr.io/google/osv-scanner:v2.6.0
PYTHON_IMAGE := python:3.14.7-slim
NODE_IMAGE := node:24.21.0
WEB_DIR := apps/web

GATEWAY_DIR := services/ibkr-realtime-gateway
CORE_DIR := services/trading-core
# Defaults for `make probe`.
SYMBOLS ?= NVDA,AAPL,META,AMD,IONQ
QUOTES ?= 5
# Defaults for `make load`: a short run. For a soak use e.g. LOAD_DURATION=10m LOAD_CLIENTS=200.
LOAD_CLIENTS ?= 50
LOAD_SYMBOLS_PER_CLIENT ?= 20
LOAD_DURATION ?= 60s
LOAD_SLOW_READERS ?= 0
LOAD_ARGS ?=
SHELL_SCRIPTS := infrastructure/scripts/bootstrap.sh infrastructure/scripts/verify-infra.sh \
                 infrastructure/postgres/init/10-create-roles.sh infrastructure/redis/start-redis.sh \
                 infrastructure/ibkr/generate-cpgw-cert.sh infrastructure/kafka/create-topics.sh \
                 infrastructure/kafka/generate-certificates.sh infrastructure/kafka/start-kafka.sh \
                 infrastructure/kafka/apply-acls.sh infrastructure/scripts/scan-images.sh \
                 infrastructure/scripts/test-resilience.sh infrastructure/scripts/test-kafka-access.sh \
                 infrastructure/scripts/benchmark.sh infrastructure/ci/cache-environment.sh \
                 infrastructure/ci/record-demo.sh infrastructure/ci/security.sh \
                 infrastructure/ci/build-images.sh infrastructure/ci/e2e.sh infrastructure/ci/delivery.sh \
                 infrastructure/ci/maintenance.sh

# Contract tests run on a copy of the sources inside the container (repository mounted read-only).
CONTRACT_COPY := mkdir -p /work/tests && cp -r /src/contracts /work/ && cp -r /src/tests/contract /work/tests/ && cd /work/tests/contract
CONTRACT_RUN := $(DOCKER_RUN) -v "$(HOST_PWD):/src:ro"
# Go service checks run on a copy of the service plus the contracts (its integration tests validate against them).
GATEWAY_COPY := mkdir -p /work/services && cp -r /src/contracts /work/ && cp -r /src/$(GATEWAY_DIR) /work/services/ && cd /work/$(GATEWAY_DIR)
# Trading Core builds run on a copy of the service plus the contracts and infrastructure its tests use. The Docker
# socket lets the integration tests start PostgreSQL and Redis with Testcontainers.
CORE_COPY := mkdir -p /work/services /work/tests && cp -r /src/contracts /src/infrastructure /work/ && cp -r /src/tests/contract /work/tests/ && cp -r /src/$(CORE_DIR) /work/services/ && rm -rf /work/$(CORE_DIR)/target && cd /work/$(CORE_DIR)
# CI may bind download caches; local development keeps the existing named volumes.
MAVEN_CACHE ?= trading-terminal-m2
GO_MODULE_CACHE ?= trading-terminal-gomod
GO_BUILD_CACHE ?= trading-terminal-gobuild
NPM_CACHE ?= trading-terminal-npm
PIP_CACHE ?= trading-terminal-pip
AI_TOOLS ?= trading-terminal-ai-tools
MAVEN_RUN := $(DOCKER_RUN) -v "$(HOST_PWD):/src:ro" -v "$(MAVEN_CACHE):/root/.m2"
TESTCONTAINERS_RUN := $(MAVEN_RUN) -v /var/run/docker.sock:/var/run/docker.sock --add-host=host.docker.internal:host-gateway \
	-e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal
GO_RUN := $(DOCKER_RUN) -v "$(HOST_PWD):/src:ro" -v "$(GO_MODULE_CACHE):/go/pkg/mod" -v "$(GO_BUILD_CACHE):/root/.cache/go-build"
WEB_COPY := mkdir -p /work/apps/web && tar -C /src/$(WEB_DIR) --exclude=node_modules --exclude=.next --exclude=test-results --exclude=playwright-report -cf - . | tar -C /work/apps/web -xf - && cd /work/apps/web
WEB_RUN := $(DOCKER_RUN) -v "$(HOST_PWD):/src:ro" -v "$(NPM_CACHE):/root/.npm"
AI_COPY := mkdir -p /work/services /work/tests && cp -r /src/contracts /work/ && cp -r /src/tests/contract /work/tests/ && cp -r /src/services/ai-insights /work/services/ && cd /work/services/ai-insights
AI_RUN := $(DOCKER_RUN) -v "$(HOST_PWD):/src:ro" -v "$(PIP_CACHE):/root/.cache/pip" -v "$(AI_TOOLS):/venv"
AI_INSTALL := (test -x /venv/bin/python || python -m venv /venv) && if ! cmp -s requirements-dev.txt /venv/requirements-dev.txt; then /venv/bin/pip install --quiet --require-hashes -r requirements-dev.txt && cp requirements-dev.txt /venv/requirements-dev.txt; fi

.PHONY: help bootstrap up up-obs up-mock up-ibkr up-ibkr-paper up-ibkr-fake ibkr-certs down ps logs verify probe load test test-unit test-core test-contract test-web test-e2e \
        contract-java contract-go contract-python contract-typescript lint lint-go lint-java lint-contracts lint-web security clean \
        lint-ai test-ai eval-ai test-kafka-access test-resilience security-ai security-web security-images security-source benchmark

.PHONY: demo-record demo-publish showcase-build test-ci lint-workflows
demo-publish: ## Publish the static showcase using ignored local configuration only
	@python3 infrastructure/ci/publication.py

lint-workflows: ## Validate GitHub workflow syntax and expressions with pinned actionlint
	@$(GO_RUN) $(GO_IMAGE) sh -c 'mkdir /work && cp -r /src/.github /work/ && cd /work && go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows/*.yml'

demo-record: ## Record genuine MOCK trading in a fresh runner-owned environment
	@bash infrastructure/ci/record-demo.sh

showcase-build: ## Build a static destination-neutral showcase (recording supplied locally)
	@$(DOCKER_RUN) -v "$(HOST_PWD):/src" -w /src -e SHOWCASE_PREFIX -e SHOWCASE_MEDIA -e SHOWCASE_OUTPUT $(NODE_IMAGE) node apps/showcase/build.mjs

test-ci: ## Verify delivery security policy and static showcase boundaries
	@$(DOCKER_RUN) -v "$(HOST_PWD):/src:ro" -w /src $(PYTHON_IMAGE) python -m unittest discover -s tests/ci -v
	@$(DOCKER_RUN) -v "$(HOST_PWD):/src" -w /src $(NODE_IMAGE) node --test tests/ci/showcase.test.mjs

benchmark: ## Three bounded, read-only load runs against a disposable stack (explicit environment required)
	@bash infrastructure/scripts/benchmark.sh

.PHONY: benchmark-ai-state
benchmark-ai-state: ## Profile bounded state checks or JSON replay (no Kafka or model calls)
	@$(AI_RUN) $(PYTHON_IMAGE) sh -c '$(AI_COPY) && $(AI_INSTALL) && PYTHONPATH=src /venv/bin/python benchmarks/state.py $(STATE_BENCHMARK_ARGS)'

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

bootstrap: ## Check prerequisites; create .env and generate local secrets (never overwrites existing ones)
	@bash infrastructure/scripts/bootstrap.sh

up: bootstrap ## Start core infrastructure: PostgreSQL, Redis, Kafka
	$(COMPOSE) up -d --wait --wait-timeout $(WAIT_TIMEOUT)

up-obs: bootstrap ## Start core infrastructure plus Prometheus, Grafana, Tempo and the OpenTelemetry Collector
	$(COMPOSE_OBS) up -d --wait --wait-timeout $(WAIT_TIMEOUT)

up-mock: bootstrap ## Start the MOCK application: infrastructure, gateway, Trading Core, fixture insights and web terminal
	$(COMPOSE) up -d --wait --wait-timeout $(WAIT_TIMEOUT) postgres redis kafka
	$(COMPOSE) run --rm kafka-init
	$(COMPOSE_MOCK) up -d --build --wait --wait-timeout $(WAIT_TIMEOUT) realtime-gateway trading-core ai-insights web

up-ibkr: bootstrap ## Start the realtime gateway on IBKR market data (needs make ibkr-certs and a CP Gateway login)
	@test -s secrets/ibkr/ca.pem || { echo "up-ibkr: secrets/ibkr/ca.pem is missing - run 'make ibkr-certs' and install the certificate in the CP Gateway first"; exit 1; }
	$(COMPOSE_IBKR) up -d --build --wait --wait-timeout $(WAIT_TIMEOUT)

up-ibkr-paper: bootstrap ## Start the gateway on IBKR market data and Trading Core on the IBKR paper account (clean database; see .env)
	@test -s secrets/ibkr/ca.pem || { echo "up-ibkr-paper: secrets/ibkr/ca.pem is missing - run 'make ibkr-certs' and install the certificate in the CP Gateway first"; exit 1; }
	@grep -Eq '^IBKR_PAPER_ACCOUNT_ID=[A-Za-z0-9_-]+' .env || { echo "up-ibkr-paper: set IBKR_PAPER_ACCOUNT_ID in .env (your paper trading account ID)"; exit 1; }
	$(COMPOSE_IBKR_PAPER) up -d --build --wait --wait-timeout $(WAIT_TIMEOUT)

up-ibkr-fake: bootstrap ## Start the gateway's IBKR code path against a FAKE CP Gateway (tests; no IBKR account)
	$(COMPOSE_IBKR_FAKE) up -d --build --wait --wait-timeout $(WAIT_TIMEOUT)

ibkr-certs: ## Generate the per-machine local CA and CP Gateway certificate into ./secrets/ibkr (FORCE=1 replaces them)
	@mkdir -p secrets/ibkr
	$(DOCKER_RUN) -e FORCE=$(FORCE) -v "$(HOST_PWD)/secrets/ibkr:/out" -v "$(HOST_PWD)/infrastructure/ibkr:/scripts:ro" \
		--entrypoint sh $(MAVEN_IMAGE) /scripts/generate-cpgw-cert.sh /out

down: ## Stop and remove containers (data volumes are kept)
	$(COMPOSE_ALL) down

ps: ## Show container status
	$(COMPOSE_ALL) ps

logs: ## Follow logs (all services, or SERVICE=<name>)
	$(COMPOSE_ALL) logs -f --tail=200 $(SERVICE)

verify: ## Verify infrastructure connectivity, security posture and behavior (VERIFY_RESTARTS=1 adds restart checks)
	@bash infrastructure/scripts/verify-infra.sh

probe: ## Connect a WebSocket test client to the running mock feed (SYMBOLS=a,b QUOTES=n)
	MSYS_NO_PATHCONV=1 $(COMPOSE_MOCK) exec realtime-gateway /usr/local/bin/stream-probe \
		-url ws://127.0.0.1:8090/ws -symbols $(SYMBOLS) -quotes $(QUOTES)

load: ## Run load/soak clients against the running mock feed, with leak checks (LOAD_CLIENTS, LOAD_DURATION, LOAD_ARGS)
	MSYS_NO_PATHCONV=1 $(COMPOSE_MOCK) run --rm --no-deps --entrypoint /usr/local/bin/stream-load realtime-gateway \
		-url ws://realtime-gateway:8090/ws -metrics-url http://realtime-gateway:8091/metrics \
		-clients $(LOAD_CLIENTS) -symbols-per-client $(LOAD_SYMBOLS_PER_CLIENT) -duration $(LOAD_DURATION) \
		-slow-readers $(LOAD_SLOW_READERS) $(LOAD_ARGS)

test: test-contract test-unit test-core test-web test-ai eval-ai verify test-e2e ## Run all available checks, including the web terminal and MOCK browser workflow

test-web: ## Typecheck, unit test and production-build the web terminal in pinned Node
	@$(WEB_RUN) $(NODE_IMAGE) sh -c '$(WEB_COPY) && npm ci --ignore-scripts --no-audit --no-fund && npm run typecheck && npm test && npm run build'
	@echo "test-web: ok"

test-e2e: ## Run the Playwright MOCK workflow against the running web/core/gateway stack
	@$(COMPOSE) --profile test run --rm --no-deps playwright
	@echo "test-e2e: ok"

test-unit: ## Go unit and integration tests of the realtime gateway, with the race detector
	@$(GO_RUN) $(GO_IMAGE) sh -c '$(GATEWAY_COPY) && go test -race -count=1 ./...'
	@echo "test-unit: ok"

test-core: ## Trading Core tests: domain, application, architecture, and the PostgreSQL + Redis integration suite
	@$(TESTCONTAINERS_RUN) $(MAVEN_IMAGE) sh -c '$(CORE_COPY) && mvn -B -ntp -q verify'
	@echo "test-core: ok"

test-contract: contract-java contract-go contract-python contract-typescript ## Run the contract tests in all four languages

contract-java: ## Contract tests - Java (networknt json-schema-validator, Jackson, JUnit)
	@$(CONTRACT_RUN) -v "$(MAVEN_CACHE):/root/.m2" $(MAVEN_IMAGE) sh -c \
		'$(CONTRACT_COPY)/java && mvn -B -ntp -q test'
	@echo "contract-java: ok"

contract-go: ## Contract tests - Go (santhosh-tekuri/jsonschema)
	@$(CONTRACT_RUN) -v "$(GO_MODULE_CACHE):/go/pkg/mod" $(GO_IMAGE) sh -c \
		'$(CONTRACT_COPY)/go && go vet ./... && go test -count=1 ./...'
	@echo "contract-go: ok"

contract-python: ## Contract tests - Python (jsonschema) including the OpenAPI/AsyncAPI reference checks
	@$(CONTRACT_RUN) -v "$(PIP_CACHE):/root/.cache/pip" $(PYTHON_IMAGE) sh -c \
		'$(CONTRACT_COPY)/python && pip install --quiet --disable-pip-version-check --root-user-action=ignore -r requirements.txt && python -m unittest'
	@echo "contract-python: ok"

contract-typescript: ## Contract tests - TypeScript (Ajv) with strict type checking
	@$(CONTRACT_RUN) -v "$(NPM_CACHE):/root/.npm" $(NODE_IMAGE) sh -c \
		'$(CONTRACT_COPY)/typescript && npm ci --ignore-scripts --no-audit --no-fund --loglevel=error && npm test --silent'
	@echo "contract-typescript: ok"

lint: lint-infra lint-contracts lint-go lint-java lint-web lint-ai ## Validate infrastructure, services and contracts

lint-infra: bootstrap ## Validate Compose and shell scripts
	@echo "compose: default profile"; $(COMPOSE) config --quiet
	@echo "compose: observability profile"; $(COMPOSE_OBS) config --quiet
	@echo "compose: gateway and core profiles"; $(COMPOSE_MOCK) config --quiet
	@echo "compose: browser test profile"; $(COMPOSE) --profile gateway --profile core --profile web --profile test config --quiet
	@echo "compose: ibkr-fake override"; $(COMPOSE_IBKR_FAKE) config --quiet
	@echo "shellcheck: $(SHELL_SCRIPTS)"
	@$(DOCKER_RUN) -v "$(HOST_PWD):/mnt:ro" -w /mnt $(SHELLCHECK_IMAGE) --severity=style $(SHELL_SCRIPTS)
	@echo "lint: ok"

lint-go: ## Go formatting, go vet and golangci-lint for the realtime gateway
	@echo "go: gofmt + go vet"
	@$(GO_RUN) $(GO_IMAGE) sh -c '$(GATEWAY_COPY) && unformatted=$$(gofmt -l .) && { [ -z "$$unformatted" ] || { echo "$$unformatted"; exit 1; }; } && go vet ./...'
	@echo "go: golangci-lint"
	@$(GO_RUN) $(GOLANGCI_IMAGE) sh -c '$(GATEWAY_COPY) && golangci-lint run ./...'

lint-java: ## Trading Core: compile with warnings as errors, Maven enforcer rules, architecture rules
	@$(MAVEN_RUN) $(MAVEN_IMAGE) sh -c '$(CORE_COPY) && mvn -B -ntp -q -Dtest=ArchitectureTest -Dsurefire.failIfNoSpecifiedTests=false verify'
	@echo "lint-java: ok"

lint-web: ## ESLint and Prettier checks for the Next.js terminal
	@$(WEB_RUN) $(NODE_IMAGE) sh -c '$(WEB_COPY) && npm ci --ignore-scripts --no-audit --no-fund && npm run lint && npm run format:check'
	@echo "lint-web: ok"

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

security: security-ai security-web security-images security-source ## Scan public files, dependencies and local runtime images

security-source: security-secrets security-go security-java ## Scan Git/public files and Go/Maven dependencies
	@echo "security: ok"

.PHONY: security-secrets security-go security-java
security-secrets:
	@echo "gitleaks: git history"
	@$(DOCKER_RUN) -v "$(HOST_PWD):/repo:ro" --entrypoint sh $(GITLEAKS_IMAGE) -c \
		'git config --global --add safe.directory /repo && gitleaks git /repo --redact --no-banner'
	@echo "gitleaks: files that would be committed"
	@git ls-files -z --cached --others --exclude-standard | \
		while IFS= read -r -d '' f; do if [ -e "$$f" ] || [ -L "$$f" ]; then printf '%s\0' "$$f"; fi; done | \
		tar --null -T - -cf - | \
		$(DOCKER_RUN) -i --entrypoint sh $(GITLEAKS_IMAGE) -c \
		'mkdir -p /scan && tar -xf - -C /scan && gitleaks dir /scan --redact --no-banner'
security-go:
	@echo "govulncheck: realtime gateway and Go contract tests"
	@$(GO_RUN) $(GO_IMAGE) sh -c '$(GATEWAY_COPY) && go run $(GOVULNCHECK) ./... && cp -r /src/tests/contract/go /tmp/contract-go && cd /tmp/contract-go && go run $(GOVULNCHECK) ./...'
security-java:
	@echo "osv-scanner: Trading Core Maven dependencies (transitive)"
	@$(DOCKER_RUN) -v "$(HOST_PWD)/$(CORE_DIR):/src:ro" $(OSV_SCANNER_IMAGE) scan source /src/pom.xml

clean: ## Stop containers and DELETE all local data volumes (asks for confirmation; CONFIRM=yes skips it)
	@if [[ "$${CONFIRM:-}" != "yes" ]]; then \
		read -r -p "This deletes all local data volumes. Type 'yes' to continue: " answer; \
		[[ "$$answer" == "yes" ]] || { echo "aborted"; exit 1; }; \
	fi
	$(COMPOSE_ALL) down --volumes --remove-orphans
	@echo "clean: containers and volumes removed (.env and ./secrets are kept; delete them manually to regenerate)"

.PHONY: test-ai eval-ai lint-ai security-ai lint-infra
test-ai: ## Worker unit and real Kafka integration tests (no live inference)
	@$(AI_RUN) -v /var/run/docker.sock:/var/run/docker.sock --add-host=host.docker.internal:host-gateway -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal -e TESTCONTAINERS_RYUK_DISABLED=true $(PYTHON_IMAGE) sh -c '$(AI_COPY) && $(AI_INSTALL) && /venv/bin/python -m pytest -q --ignore=tests/eval'

eval-ai: ## Deterministic insight evaluation (not live-model quality)
	@$(AI_RUN) $(PYTHON_IMAGE) sh -c '$(AI_COPY) && $(AI_INSTALL) && /venv/bin/python -m pytest -q tests/eval'

lint-ai: ## Worker Ruff lint/format and strict mypy
	@$(AI_RUN) $(PYTHON_IMAGE) sh -c '$(AI_COPY) && $(AI_INSTALL) && /venv/bin/ruff check . && /venv/bin/ruff format --check . && /venv/bin/mypy src'

security-ai: ## Audit hash-locked Python runtime and development dependencies
	@$(AI_RUN) $(PYTHON_IMAGE) sh -c '$(AI_COPY) && $(AI_INSTALL) && /venv/bin/pip-audit --disable-pip --no-deps -r requirements-dev.txt'

security-web: ## Audit the exact frontend dependency lock, including development tools
	@$(WEB_RUN) $(NODE_IMAGE) sh -c '$(WEB_COPY) && npm audit --audit-level=high'

security-images: ## Scan local runtime images for detected High/Critical vulnerabilities
	@bash infrastructure/scripts/scan-images.sh

test-resilience: ## Bounded disruption checks; requires an explicitly configured disposable MOCK stack
	@bash infrastructure/scripts/test-resilience.sh

test-kafka-access: ## Broker-observed identity, ACL and trust checks on a disposable Kafka project
	@bash infrastructure/scripts/test-kafka-access.sh
