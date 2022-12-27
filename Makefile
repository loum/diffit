.SILENT:
.DEFAULT_GOAL := help

MAKESTER__CONTAINER_NAME := diffit-spark

include makester/makefiles/makester.mk
MAKESTER__PACKAGE_NAME := diffit

ifeq ($(MAKESTER__ARCH), arm64)
DOCKER_PLATFORM := linux/arm64/v8
else
DOCKER_PLATFORM := linux/amd64
endif

MAKESTER__WHEEL := .wheelhouse

_venv-init: py-venv-clear py-venv-init

# Install optional packages for development.
init-dev: _venv-init py-install-makester
	MAKESTER__PIP_INSTALL_EXTRAS=dev $(MAKE) gitversion-release py-install-extras

# Streamlined production packages.
init: py-venv-clear py-venv-init gitversion-release
	$(MAKE) py-install

MAKESTER__VERSION_FILE := $(MAKESTER__PYTHON_PROJECT_ROOT)/VERSION

TESTS := tests
tests:
	$(MAKESTER__PYTHON) -m pytest\
 --override-ini log_cli=true\
 --override-ini  junit_family=xunit2\
 --log-cli-level=INFO -vv\
 --exitfirst\
 --cov-config tests/.coveragerc\
 --pythonwarnings ignore\
 --cov src\
 -p tests.diffit.dataframes\
 --junitxml junit.xml $(TESTS)

# Tagging convention used: <spark-version>-<diffit-version>-<image-release-number>
MAKESTER__RELEASE_NUMBER ?= 1

# Tag the image build
export MAKESTER__IMAGE_TAG_ALIAS := $(MAKESTER__SERVICE_NAME):$(MAKESTER__RELEASE_VERSION)-$(MAKESTER__RELEASE_NUMBER)

py-distribution: MAKESTER__WHEEL := .wheelhouse

lint:
	-@pylint $(MAKESTER__PROJECT_DIR)/src

dep-builder:
	$(MAKESTER__PIP) install --upgrade --target .dependencies .

dep-package: _dep-package-setup dep-builder _dep-package
_dep-package-setup:
	$(info ### Building the spark-submit python dependencies zip file)
	@$(shell which mkdir) -pv $(MAKESTER__PROJECT_DIR)/.dependencies
_dep-package:
	cd $(MAKESTER__PROJECT_DIR)/.dependencies && zip -r $(MAKESTER__PROJECT_DIR)/docker/files/python/dependencies.zip .

CMD ?= --help
diffit:
	@diffit $(CMD)

DOCKER_BUILDX_BUILDER := multiarch
SPARK_PSEUDO_BASE_IMAGE := 3.3.4-3.3.1
MAKESTER__BUILD_COMMAND := --rm --no-cache\
 --builder $(DOCKER_BUILDX_BUILDER)\
 --platform $(DOCKER_PLATFORM)\
 --build-arg RELEASE_VERSION=$(MAKESTER__RELEASE_VERSION)\
 --build-arg UBUNTU_BASE_IMAGE=$(UBUNTU_BASE_IMAGE)\
 --build-arg SPARK_PSEUDO_BASE_IMAGE=$(SPARK_PSEUDO_BASE_IMAGE)\
 --load\
 --tag $(MAKESTER__IMAGE_TAG_ALIAS)\
 -f docker/Dockerfile .

diffit-image-build: gitversion-release py-distribution dep-package image-buildx

ifndef SCHEMA
SCHEMA := $(PWD)/docker/files/schema
endif
ifndef DATA
DATA := $(PWD)/docker/files/data
endif
ifndef OUTPUT_PATH
OUTPUT_PATH := file:///tmp/data/out
endif
MAKESTER__RUN_COMMAND = $(MAKESTER__DOCKER) run\
 --rm -d\
 --platform $(DOCKER_PLATFORM)\
 --publish 8032:8032\
 --publish 7077:7077\
 --publish 8080:8080\
 --publish 8088:8088\
 --publish 8042:8042\
 --publish 18080:18080\
 --env CORE_SITE__HADOOP_TMP_DIR=/tmp\
 --env HDFS_SITE__DFS_REPLICATION=1\
 --env YARN_SITE__YARN_NODEMANAGER_RESOURCE_DETECT_HARDWARE_CAPABILITIES=true\
 --env YARN_SITE__YARN_LOG_AGGREGATION_ENABLE=true\
 --env OUTPUT_PATH=$(OUTPUT_PATH)\
 --env NUM_EXECUTORS=6\
 --env EXECUTOR_CORES=2\
 --env EXECUTOR_MEMORY=768m\
 --volume $(PWD)/docker/files/python:/data\
 --volume $(SCHEMA):/tmp/schema\
 --volume $(DATA):/tmp/data\
 --hostname $(MAKESTER__CONTAINER_NAME)\
 --name $(MAKESTER__CONTAINER_NAME)\
 $(MAKESTER__IMAGE_TAG_ALIAS) $(CMD)

MAKESTER__COMPOSE_FILES = -f docker/docker-compose.yml

pyspark:
	@PYSPARK_PYTHON=$(MAKESTER__PYTHON) pyspark --driver-memory=2G --conf spark.sql.session.timeZone=UTC

help: makester-help docker-help py-help versioning-help
	@echo "(Makefile)\n\
  diffit-image-build   Diffit tooling container image builder\n\
  init                 Build the local Python-based virtual environment\n\
  init-dev             Build the local Python-based virtual environment (development)\n\
  lint                 Lint the code base\n\
  pyspark              Start the PyPI pyspark interpreter in virtual env context\n\
  tests                Run code test suite\n"

.PHONY: help tests
