#!/bin/bash

REGISTRY=hub.docker.com
REPOSITORY=container-registry
NAME=preservation-services
REVISION:=$(shell git rev-parse --short=7 HEAD)
BRANCH:= $(subst /,_,$(shell git rev-parse --abbrev-ref HEAD))
PUSHBRANCH = $(subst /,_,$(TRAVIS_BRANCH))
TAG=$(name):$(REVISION)
APT_ENV:='test'
APT_SERVICES_CONFIG_DIR:=./

#APPS:=$(basename $(find ./apps -name *.go) .go)
#	@for app in $$(find ./apps -name *.go); do \
		APP_NAME=$$(basename $$app .go); \

ifdef TRAVIS
override BRANCH=$(PUSHBRANCH)
endif

OUTPUT_DIR:=go-bin

DOCKERAPPS := redis nsqlookup nsqd nsqadmin minio
DOCKER_TAG_NAME:=${REVISION}-${BRANCH}

#
# HELP
# This will output the help for each task
# thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help build publish release push clean run unittest init

help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

get_apps:
	echo $(APPS)

docker_login:
	if [ ! -z "$(DOCKER_USER)" ]; then echo $(DOCKER_PWD) | docker login --username $(DOCKER_USER) --password-stdin || docker login $(REGISTRY); fi

init: ## Start dependent services for integration tests and development
	@for folder in [ "bin" "logs" "minio" "nsq" "redis" "restore" ]; do \
		mkdir -p /tmp/$$folder; \
	done
#	- @docker network create --attachable ps_test_network
	- @docker run --name redis -d -p 6379 redis:6.0.1-alpine redis-server --appendonly yes
	- @docker run --name nsqlookup -d -p 4160 nsqio/nsq:v1.2.0 nsqlookupd
	- @docker run --name nsqd -d -p 4151 nsqio/nsq:v1.2.0 nsqd --lookupd-tcp-address=127.0.0.1:4160 --data-path /tmp/nsq
	- @docker run --name nsqadmin -d -p 4171:4171 nsqio/nsq:v1.2.0 nsqadmin --lookupd-http-address=nslookupd:4161
	- @docker run --name minio -d -p 9899 minio/minio minio server --quiet --address=127.0.0.1:9899 ~/tmp/minio
	- @docker run --name pharos -d -e PHAROS_ROOT

init_clean:
	@for app in $(DOCKERAPPS); do \
		docker stop $$app; \
		docker rm $$app; \
	done

revision: ## Show me the git hash
	@echo "Revision: ${REVISION}"
	@echo "Branch: ${BRANCH}"

build-bin: ## Build the Preservation-Services binaries
	@for app in $$(find ./apps -name *.go); do \
		APP_NAME=$$(basename $$app .go); \
		echo "Building $$APP_NAME" binary; \
		$$(CGO_ENABLED=0 go build -ldflags '-w' -o ${OUTPUT_DIR}/$$APP_NAME $$app); \
	done

build: ## Build the Preservation-Services containers
	@echo "Branch: ${BRANCH}"
	@mkdir -p ${OUTPUT_DIR};
	@for app in $$(find ./apps -name *.go); do \
		APP_NAME=$$(basename $$app .go); \
		echo "Building $$APP_NAME" Docker container ${DOCKER_TAG_NAME}; \
		docker build --platform=linux/amd64,linux/arm64 --build-arg PSERVICE=$$APP_NAME --build-arg OUTPUT_DIR=${OUTPUT_DIR} -t aptrust/$$APP_NAME:${DOCKER_TAG_NAME} -t aptrust/$$APP_NAME -f Dockerfile.build . ; \
	done

up: ## Start Preservation service containers
	docker network create --attachable gateway || true
	docker-compose --env-file .env.docker up

stop: ## Stop Exchange+NSQ containers
	docker-compose stop

down: ## Stop and remove all Exchange+NSQ containers, networks, images, and volumes
	docker-compose --env-file .env.docker down
	# Disable for now since it would potentially remove production network
	# docker network rm gateway

run: ## Run Exchange service in foreground
	docker run aptrust/$(NAME)_$(filter-out $@, $(MAKECMDGOALS))

runcmd: ## Run a one time command. Takes exchange service name as argument.
	@echo "Need to pass in exchange service and cmd. e.g. make runcmd apt_record bash"
	docker run -it aptrust/$(NAME)_$(filter-out $@, $(MAKECMDGOALS))

%:
	@:

unittest: init ## Run unit tests in non Docker setup
	go clean -testcache
	go test -p 1 ./...
	init_clean

test-ci: ## Run unit tests in CI
	docker run exchange-ci-test

publish: docker_login
	@for app in $$(find ./apps -name *.go); do \
		APP_NAME=$$(basename $$app .go); \
		echo "Publishing $$APP_NAME:$(REVISION)-$(BRANCH)"; \
		docker push aptrust/$$APP_NAME:$(REVISION)-$(BRANCH);\
	done

# Docker release - build, tag and push the container
release: build publish ## Create a release by building and publishing tagged containers to Gitlab

push: ## Push the Docker image up to the registry
#	docker push  $(registry)/$(repository)/$(tag)
	@echo "TBD"

update-template: ## Update Cloudformation template with latest container version
	@echo "Overwriting container revision and branch from the CFN template to the CFN deployment YAML document."
	sed 's/multi/$(REVISION)-$(BRANCH)/g' cfn/cfn-preserv-cluster.tmpl > cfn/cfn-preserv-cluster.yml

clean: ## Clean the generated/compiles files
	@echo "TBD"
