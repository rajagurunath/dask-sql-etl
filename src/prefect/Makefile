#!/usr/bin/make

.DEFAULT_GOAL := help
# COLORS

BOLD := $(shell tput bold)
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)
TARGET_MAX_CHAR_NUM=30

.PHONY: venv
## create a virtual environment for development
venv:
	virtualenv -p python3 ../../venv
	source ../../venv/bin/activate && pip install pip --upgrade && pip install -r requirements.txt


.PHONY: clean
## clean all files using .gitignore rules
clean:
	@git clean -X -f -d


.PHONY: scrub
## clean all files, even untracked files
scrub:
	git clean -x -f -d


.PHONY: start_server
## starts prefect server
start_server:
## make sure you have docker installed and docker-deamon was up and running
	prefect backend server

	prefect server start


.PHONY: start_agent
## starts prefect agent
start_agent:
	prefect agent local start


PHONY: stop_server
## stops prefect server
stop_server:
	prefect server stop




PHONY: start_dask_scheduler
## starts dask-scheduler
start_dask_scheduler:
	dask-scheduler 

PHONY: start_dask_worker
## starts dask-scheduler
start_dask_worker:
	dask-worker localhost:8786


PHONY: run_etl
## Run's example Dask-sql ETL
run_etl:
	prefect create project dask-sql
	python sql_etl.py



.PHONY: help
## Show help
help:
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@grep "^# help\:" Makefile | grep -v grep | sed 's/\# help\: //' | sed 's/\# help\://'

	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")-1); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
			printf "  ${YELLOW}%-$(TARGET_MAX_CHAR_NUM)s${RESET} ${GREEN}%s${RESET}\n", helpCommand, helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

