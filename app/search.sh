#!/bin/bash
set -euo pipefail

echo "Searching for documents using BM25"

wait_for_hdfs_ready() {
    for _ in $(seq 1 30); do
        if hdfs dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode is OFF"; then
            return 0
        fi
        hdfs dfsadmin -safemode leave >/dev/null 2>&1 || true
        sleep 2
    done
    echo "HDFS is still in safe mode."
    exit 1
}

wait_for_hdfs_ready

spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 512m \
    --conf spark.driver.memoryOverhead=128m \
    --executor-memory 512m \
    --conf spark.executor.memoryOverhead=128m \
    --conf spark.executor.cores=1 \
    --conf spark.executor.instances=1 \
    --conf spark.yarn.am.memory=384m \
    --conf spark.yarn.am.memoryOverhead=384m \
    --conf spark.yarn.am.cores=1 \
    --conf spark.default.parallelism=1 \
    --conf spark.sql.shuffle.partitions=1 \
    --conf spark.yarn.archive=hdfs:///apps/spark/spark-yarn-archive.jar \
    --py-files /app/engine_utils.py \
    query.py "$@"
