#!/bin/bash

set -euo pipefail

# DOWNLOAD a.parquet or any parquet file before you run this

if hdfs dfs -test -e /data && hdfs dfs -test -e /input/data; then
    echo "Reusing existing HDFS /data and /input/data."
    echo "Prepared HDFS paths:"
    hdfs dfs -ls /data | head -n 5
    hdfs dfs -ls /input/data
    echo "done data preparation!"
    exit 0
fi

if [ -f /app/a.parquet ]; then
    echo "Uploading /app/a.parquet to HDFS"
    hdfs dfs -put -f /app/a.parquet /
else
    echo "No /app/a.parquet found. Reusing bundled text corpus in /app/data."
fi

hdfs dfs -rm -r -f /data >/dev/null 2>&1 || true
hdfs dfs -rm -r -f /input/data >/dev/null 2>&1 || true

echo "Building HDFS /data and single-partition search input in HDFS /input/data"
python3 prepare_data.py

echo "Prepared HDFS paths:"
hdfs dfs -ls /data | head -n 5
hdfs dfs -ls /input/data
echo "done data preparation!"
