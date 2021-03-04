.DEFAULT_GOAL := help
.PHONY: help

VERSION ?= latest
DOCKER_ORG ?= federatedswarm
DOCKER_REPO ?= batch

help:  ## List out all commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

build:  # Build Docker Image
	@docker build -t $(DOCKER_ORG)/$(DOCKER_REPO):$(VERSION) .

push:  # Push Docker Image
	@docker push $(DOCKER_ORG)/$(DOCKER_REPO):$(VERSION)
