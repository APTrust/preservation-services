#!/bin/bash

REGISTRY=registry.gitlab.com/aptrust
REPOSITORY=container-registry
NAME=preservation-services
REVISION:=$(shell git rev-parse --short=7 HEAD)
BRANCH = $(subst /,_,$(shell git rev-parse --abbrev-ref HEAD))
PUSHBRANCH = $(subst /,_,$(TRAVIS_BRANCH))
TAG=$(name):$(REVISION)
APT_SERVICES_CONFIG:='test'

DOCKERAPPS := redis nsqlookup nsqd nsqadmin minio
DOCKER_TAG_NAME=${REVISION}-${BRANCH}

ifdef TRAVIS
override BRANCH=$(PUSHBRANCH)
endif

#
# HELP
# This will output the help for each task
# thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help build publish release push clean run unittest init

help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

init: ## Start dependent services for integration tests and development
	@for folder in [ "bin" "logs" "minio" "nsq" "redis" "restore" ]; do \
		mkdir -p /tmp/$$folder; \
	done
	- @docker run --name redis -d -p 6379:6379 redis
	- @docker run --name nsqlookup -d -p 4160:4160 nsqio/nsq:v1.2.0 nsqlookupd
	- @docker run --name nsqd -d -p 4151:4151 nsqio/nsq:v1.2.0 nsqd --lookupd-tcp-address=127.0.0.1:4160
	- @docker run --name nsqadmin -d -p 4171:4171 nsqio/nsq:v1.2.0 nsqadmin --lookupd-http-address=127.0.0.1:4161
	- @docker run --name minio -d -p 9899:9899 minio/minio minio server --quiet --address=127.0.0.1:9899 ~/tmp/minio

init_clean:
	@for app in $(DOCKERAPPS); do \
		docker stop $$app; \
		docker rm $$app; \
	done


revision: ## Show me the git hash
	@echo "Revision: ${REVISION}"
	@echo "Branch: ${BRANCH}"

build: ## Build the Exchange containers
	@echo "Branch: ${BRANCH}"
#	@for app in $(APP_LIST:apps/%=%); do \
		echo $$app; \
/		docker build --build-arg EX_SERVICE=$$app -t aptrust/$(NAME)_$$app -t $(NAME)_$$app:$(REVISION) -t $(REGISTRY)/$(REPOSITORY)/$(NAME)_$$app:$(REVISION)-$(BRANCH) -f Dockerfile-build .; \
	done

up: ## Start Preservation service containers
	docker-compose up

stop: ## Stop Exchange+NSQ containers
	docker-compose stop

down: ## Stop and remove all Exchange+NSQ containers, networks, images, and volumes
	docker-compose down -v

run: ## Run Exchange service in foreground
	docker run aptrust/$(NAME)_$(filter-out $@, $(MAKECMDGOALS))

runcmd: ## Run a one time command. Takes exchange service name as argument.
	@echo "Need to pass in exchange service and cmd. e.g. make runcmd apt_record bash"
	docker run -it aptrust/$(NAME)_$(filter-out $@, $(MAKECMDGOALS))

%:
	@:

unittest: init ## Run unit tests in non Docker setup
	go test ./...

test-ci: ## Run unit tests in CI
	docker run exchange-ci-test

publish:
	docker login $(REGISTRY)
	@for app in $(APP_LIST:apps/%=%); \
	do \
		echo "Publishing $$app:$(REVISION)-$(BRANCH)"; \
		docker push $(REGISTRY)/$(REPOSITORY)/$(NAME)_$$app:$(REVISION)-$(BRANCH);\
	done

publish-ci:
	@echo $(DOCKER_PWD) | docker login -u $(DOCKER_USER) --password-stdin $(REGISTRY)
	@for app in $(APP_LIST:apps/%=%); \
	do \
	echo "Publishing $$app:$(REVISION)-$(PUSHBRANCH)"; \
		docker push $(REGISTRY)/$(REPOSITORY)/$(NAME)_$$app:$(REVISION)-$(PUSHBRANCH);\
	done

# Docker release - build, tag and push the container
release: build publish ## Create a release by building and publishing tagged containers to Gitlab

# Docker release - build, tag and push the container
release-ci: build publish-ci ## Create a release by building and publishing tagged containers to Gitlab


push: ## Push the Docker image up to the registry
#	docker push  $(registry)/$(repository)/$(tag)
	@echo "TBD"

clean: ## Clean the generated/compiles files
	@echo "TBD"
