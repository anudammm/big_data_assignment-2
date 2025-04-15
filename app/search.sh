#!/bin/bash
echo "This script will search for documents given the query using Spark RDD"

# Check if query is provided
if [ -z "$1" ]; then
    echo "Usage: search.sh <query>"
    exit 1
fi

source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 

# Python of the executor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

# Run the PySpark application
spark-submit --master yarn --archives /app/.venv.tar.gz#.venv query.py "$1"