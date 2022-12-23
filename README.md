# Diff-it: Data Differ
- [Overview](#Overview)
- [Prerequisites](#Prerequisites)
  - [Extras for macOS](#Extras-for-macOS)
- [Getting Started](#Getting-Started)
- [(macOS Users) Upgrading GNU Make](#(macOS-Users)Upgrading-GNU-Make)
  - [Creating the Local Environment](#Creating-the-Local-Environment)
  - [Local Environment Maintenance](#Local-Environment-Maintenance)
- [Help](#Help)
- [Running the Test Harness](#Running-the-Test-Harness)
- [Docker Image Management](#Docker-Image-Management)
  - [Image Build](#Image-Build)
  - [Validate the Image Build](#Validate-the-Image-Build)
  - [Image Searches](#Image-Searches)
  - [Docker Image Version Tag](#Docker-Image-Version-Tag)
- [Useful `make` Commands](#Useful-make-Commands)
- [FAQs](#FAQs)

## Overview
`diffit` will report differences between two data sets with similar schema.

Refer to [Diffit's documentation](https://loum.github.io/diffit/) for detailed instructions.

## Prerequisites
- [GNU make](https://www.gnu.org/software/make/manual/make.html)
- Python 3 Interpreter. [We recommend installing pyenv](https://github.com/pyenv/pyenv)
- [Docker](https://www.docker.com/)

## Getting Started
[Makester](https://loum.github.io/makester/) is used as the Integrated Developer Platform.

### (macOS Users only) Upgrading GNU Make
Follow [these notes](https://loum.github.io/makester/macos/#upgrading-gnu-make-macos) to get [GNU make](https://www.gnu.org/software/make/manual/make.html).
 
### Creating the Local Environment
Get the code and change into the top level `git` project directory:
```
git clone git@github.com:loum/diffit.git && cd diffit
```

> **_NOTE:_** Run all commands from the top-level directory of the `git` repository.

For first-time setup, get the [Makester project](https://github.com/loum/makester.git):
```
git submodule update --init
```

Initialise the environment:
```
make init-dev
```

#### Local Environment Maintenance
Keep [Makester project](https://github.com/loum/makester.git) up-to-date with:
```
git submodule update --remote --merge
```

## Help
There should be a `make` target to get most things done. Check the help for more information:
```
make help
```

## Running the Test Harness
We use [pytest](https://docs.pytest.org/en/latest/). To run the tests:
```
make tests
```

## Docker Image Management
### Image Build
To build the Docker image against Apache Spark version `SPARK_VERSION` (defaults to `3.2.1`):
```
make build-image
```
You can target a specific Apache Spark release by setting `SPARK_VERSION`. For example:
```
SPARK_VERSION=3.0.3 make build image
```
The  tag naming policy follows `<spark-version>-<diffit-version>-<image-release-number>`.

### Validate the Image Build
A convenience target is provided for you to shake-out the new Docker image build against the `Dummy` schema:
```
make run
```
You can view the status of the job with:
```
make container-logs
```
On successful completion the `diffit` output will be written to `docker/files/data/out`

### Image Searches
Search for existing Docker image tags with command:
```
make image-search
```
### Docker Image Version Tag
```
make image-tag-version
```
Tagging convention used is:
```
<SBT_VERSION>-<RELEASE_VERSION>-<MAKESTER__RELEASE_NUMBER>
```
## FAQs
**_Q. Why do I get `WARNING: An illegal reflective access operation has occurred`?_**
Seems to be related to the JVM version being used. Java 8 will suppress the warning. To check available Java versions on your Mac try `/usr/libexec/java_home -V`. Then:
```
export JAVA_HOME=$(/usr/libexec/java_home -v <java_version>)
```

---
[top](#Diff-it-Data-Differ)
