.SILENT:
.DEFAULT_GOAL := help

MAKESTER__CONTAINER_NAME := diffit-spark

include makester/makefiles/makester.mk
include makester/makefiles/python-venv.mk
include makester/makefiles/docker.mk
include makester/makefiles/compose.mk
include makester/makefiles/versioning.mk

UNAME := $(shell uname)
ifeq ($(UNAME), Darwin)
LOCAL_IP := $(shell ipconfig getifaddr en0)
DOCKER_PLATFORM := --platform linux/amd64
else ifeq ($(UNAME), Linux)
LOCAL_IP := $(shell hostname -I | awk '{print $$1}')
endif

# Update the README if changing the default Spark version.
SPARK_VERSION := 3.2.1
# Tagging convention used: <spark-version>-<workflow-version>-<image-release-number>
MAKESTER__VERSION := $(SPARK_VERSION)-$(RELEASE_VERSION)
MAKESTER__RELEASE_NUMBER ?= 1

# Tag the image build
export MAKESTER__IMAGE_TAG_ALIAS = $(MAKESTER__SERVICE_NAME):$(MAKESTER__VERSION)-$(MAKESTER__RELEASE_NUMBER)

# Add current Python virtual environment to path.
export PATH := 3env/bin:$(PATH)
export PYTHONPATH := src

# APP_ENV is used in setup.py.
ifndef APP_ENV
export APP_ENV := local
else
export APP_ENV := $(APP_ENV)
endif

init: WHEEL := .wheelhouse
init: clear-env makester-requirements
	$(info ### Installing "$(MAKESTER__PROJECT_NAME)" and dependencies ...)
	$(MAKE) pip-editable

TESTS := tests
tests:
	$(PYTHON) -m pytest\
 --override-ini log_cli=true\
 --override-ini  junit_family=xunit2\
 --log-cli-level=INFO -svv\
 --exitfirst\
 --cov-config tests/.coveragerc\
 --pythonwarnings ignore\
 --cov src\
 -p tests.dataframes\
 --junitxml junit.xml $(TESTS)

package: WHEEL = .wheelhouse
package: APP_ENV = prod

deps:
	pipdeptree

lint:
	-@pylint $(MAKESTER__PROJECT_DIR)/src

dep-builder: APP_ENV = prod
dep-builder: PIP_INSTALL = --upgrade --target .dependencies .
dep-builder: init-env

dep-package: dep-builder
	$(info ### Building the spark-submit python dependencies zip file)
	@cd $(MAKESTER__PROJECT_DIR)/.dependencies\
 && zip -r $(MAKESTER__PROJECT_DIR)/docker/files/python/dependencies.zip *

CMD ?= --help
diffit:
	@src/bin/diffit $(CMD)

diffit-schema-list: CMD = schema list
diffit-schema-list: diffit

UBUNTU_BASE_IMAGE := focal-20220426
SPARK_PSEUDO_BASE_IMAGE := 3.3.2-3.2.1
MAKESTER__BUILD_COMMAND = $(DOCKER) build --rm\
 --no-cache\
 --build-arg UBUNTU_BASE_IMAGE=$(UBUNTU_BASE_IMAGE)\
 --build-arg SPARK_PSEUDO_BASE_IMAGE=$(SPARK_PSEUDO_BASE_IMAGE)\
 -t $(MAKESTER__IMAGE_TAG_ALIAS) -f docker/Dockerfile .
build-image: dep-package

ifndef SCHEMA
SCHEMA := Dummy
endif
ifndef LEFT
LEFT := file:///tmp/data/left
endif
ifndef RIGHT
RIGHT := file:///tmp/data/right
endif
ifndef DATA
DATA := $(PWD)/docker/files/data
endif
ifndef OUTPUT_PATH
OUTPUT_PATH := file:///tmp/data/out
endif
MAKESTER__RUN_COMMAND = $(DOCKER) run\
 $(DOCKER_PLATFORM)\
 --rm -d\
 --publish 8032:8032\
 --publish 7077:7077\
 --publish 8080:8080\
 --publish 8088:8088\
 --publish 8042:8042\
 --publish 18080:18080\
 --env CORE_SITE__HADOOP_TMP_DIR=/tmp\
 --env HDFS_SITE__DFS_REPLICATION=1\
 --env YARN_SITE__YARN_NODEMANAGER_RESOURCE_DETECT_HARDWARE_CAPABILITIES=true\
 --env DIFFER_SCHEMA=$(SCHEMA)\
 --env LEFT_DATA_SOURCE=$(LEFT)\
 --env RIGHT_DATA_SOURCE=$(RIGHT)\
 --env OUTPUT_PATH=$(OUTPUT_PATH)\
 --env AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID)\
 --env AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY)\
 --env AWS_SESSION_TOKEN=$(AWS_SESSION_TOKEN)\
 --env NUM_EXECUTORS=6\
 --env EXECUTOR_CORES=2\
 --env EXECUTOR_MEMORY=768m\
 --env COLS_TO_DROP="$(COLS_TO_DROP)"\
 --env RANGE_FILTER_COLUMN="$(RANGE_FILTER_COLUMN)"\
 --env RANGE_FILTER_LOWER="$(RANGE_FILTER_LOWER)"\
 --env RANGE_FILTER_UPPER="$(RANGE_FILTER_UPPER)"\
 --env RANGE_FILTER_FORCE="$(RANGE_FILTER_FORCE)"\
 --volume $(HOME)/.aws:/home/hdfs/.aws\
 --volume $(PWD)/docker/files/python:/data\
 --volume $(DATA):/tmp/data\
 --hostname $(MAKESTER__CONTAINER_NAME)\
 --name $(MAKESTER__CONTAINER_NAME)\
 $(MAKESTER__IMAGE_TAG_ALIAS)

ifndef JUPYTER_SERVER_PORT:
JUPYTER_SERVER_PORT := 8889
endif
export JUPYTER_PORT = $(JUPYTER_SERVER_PORT)

backoff:
	@$(PYTHON) makester/scripts/backoff -d "YARN ResourceManager" -p 8032 localhost
	@$(PYTHON) makester/scripts/backoff -d "YARN ResourceManager webapp UI" -p 8088 localhost
	@$(PYTHON) makester/scripts/backoff -d "YARN NodeManager webapp UI" -p 8042 localhost
	@$(PYTHON) makester/scripts/backoff -d "Spark HistoryServer web UI port" -p 18080 localhost
	@$(PYTHON) makester/scripts/backoff -d "Jupyter dashboard" -p $(JUPYTER_PORT) localhost

MAKESTER__COMPOSE_FILES = -f docker/docker-compose.yml

stack-up stack-down:
stack-up: compose-up backoff stack-server

stack-down: compose-down

stack-server:
	@$(DOCKER) exec -ti diffit-jupyter bash -c "jupyter notebook list"

pyspark:
	@pyspark --driver-memory=2G --conf spark.sql.session.timeZone=UTC

help: makester-help python-venv-help docker-help compose-help
	@echo "(Makefile)\n\
  init                 Build the local Python-based virtual environment\n\
  deps                 Display PyPI package dependency tree\n\
  lint                 Lint the code base\n\
  tests                Run code test suite\n\
  pypspark             Start the PyPI pyspark interpreter in virtual env context\n\
  stack-up             Create local Jupyter Notebook server infrastructure and intialisation\n\
  stack-server         Get local Jupyter Notebook server URL\n\
  stack-down           Destroy local Jupyter Notebook server infrastructure\n\
  diffit-schema-list   Show the diffit tool schemas\n"

.PHONY: help tests
