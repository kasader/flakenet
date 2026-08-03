.PHONY: help ## Displays this help message.
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(firstword $(MAKEFILE_LIST)) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

## Set default command of make to help, so that running make will output help texts.
.DEFAULT_GOAL := help

.PHONY: init
init: ## Set up git pre-commit hook.
	git config core.hooksPath .githooks

.PHONY: ci
ci: fmt-check lint test vuln ## Runs the checks CI runs.

.PHONY: fmt
fmt: ## Runs code formatting.
	golangci-lint fmt ./...

.PHONY: fmt-check
fmt-check: ## Fails if any file needs formatting, without rewriting it.
	golangci-lint fmt --diff ./...

.PHONY: lint
lint: ## Runs golangci-lint for static code analysis.
	golangci-lint run ./...

.PHONY: lint-fast
lint-fast: ## Runs golangci-lint on issues introduced since the previous commit.
	golangci-lint run --new-from-rev=HEAD~1 ./...

.PHONY: test
test: ## Build and run all tests.
	go test -v -race ./...

.PHONY: cover
cover: ## Reports total test coverage.
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

.PHONY: vuln
vuln: ## Scans for known vulnerabilities via govulncheck.
	govulncheck ./...

.PHONY: tidy
tidy: ## Sync go.mod/go.sum.
	go mod tidy

.PHONY: docs
docs: ## Serves package documentation locally.
	go doc -http
