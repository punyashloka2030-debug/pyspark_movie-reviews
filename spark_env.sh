#!/usr/bin/env bash

# Load the project environment and the user-local JDK used by Spark.
# Usage: source ./spark_env.sh

_PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${_PROJECT_ROOT}/.venv/bin/activate"

export JAVA_HOME="${HOME}/.local/jdks/temurin-17"
export PATH="${JAVA_HOME}/bin:${PATH}"
export SPARK_HOME="$(${_PROJECT_ROOT}/.venv/bin/python -c 'import pathlib, pyspark; print(pathlib.Path(pyspark.__file__).parent)')"
export PYSPARK_PYTHON="${_PROJECT_ROOT}/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="${_PROJECT_ROOT}/.venv/bin/python"
export SPARK_LOCAL_IP="127.0.0.1"

unset _PROJECT_ROOT
