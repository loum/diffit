# Diff-it: Data Differ
- [Overview](#overview)
- [Prerequisites](#prerequisites)
  - [Extras for macOS](#extras-for-macos)
- [Getting Started](#getting-started)
- [(macOS Users) Upgrading GNU Make](#(macos-users)upgrading-gnu-make)
  - [Creating the Local Environment](#creating-the-local-environment)
  - [Local Environment Maintenance](#local-environment-maintenance)
- [Help](#help)
- [Running the Test Harness](#running-the-test-harness)
- [FAQs](#faqs)

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

## FAQs
**_Q. Why do I get `WARNING: An illegal reflective access operation has occurred`?_**
Seems to be related to the JVM version being used. Java 8 will suppress the warning. To check available Java versions on your Mac try `/usr/libexec/java_home -V`. Then:
```
export JAVA_HOME=$(/usr/libexec/java_home -v <java_version>)
```

---
[top](#Diff-it-Data-Differ)
