.PHONY: default
default: | help

.PHONY: install-build-tools
install-runtime-tools: ## Install required tools for running and using tools
	pip install pytest

.PHONY: test
test: ## Run tests
	pytest tests/

.PHONY: clear-logs
clear-logs: ## sletter alle loggfiler etter kjøring
	rm -rf log/*
    
.PHONY: bump-version-patch
bump-version-patch: ## Bump patch version, e.g. 0.0.1 -> 0.0.2
	bumpversion patch

.PHONY: bump-version-minor
bump-version-minor: ## Bump minor version, e.g. 0.0.1 -> 0.1.0
	bumpversion minor

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
