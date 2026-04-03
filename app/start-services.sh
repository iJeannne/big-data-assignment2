#!/bin/bash
set -euo pipefail
# Start the Hadoop services using the mounted configuration files.

# starting HDFS daemons
$HADOOP_HOME/sbin/start-dfs.sh

# starting Yarn daemons
$HADOOP_HOME/sbin/start-yarn.sh
# yarn --daemon start resourcemanager

# Start mapreduce history server
mapred --daemon start historyserver


# track process IDs of services
jps -lm

# subtool to perform administrator functions on HDFS
# outputs a brief report on the overall HDFS filesystem
hdfs dfsadmin -report

# If namenode in safemode then leave it
hdfs dfsadmin -safemode leave

# create a single Spark archive in HDFS so YARN localizes one file instead of hundreds
SPARK_ARCHIVE_LOCAL=/tmp/spark-yarn-archive.jar
SPARK_ARCHIVE_HDFS=/apps/spark/spark-yarn-archive.jar
jar -cf "$SPARK_ARCHIVE_LOCAL" -C "$SPARK_HOME/jars" .
hdfs dfs -mkdir -p /apps/spark
hdfs dfs -put -f "$SPARK_ARCHIVE_LOCAL" "$SPARK_ARCHIVE_HDFS"


# print version of Scala of Spark
scala -version

# track process IDs of services
jps -lm

# Create a directory for root user on HDFS
hdfs dfs -mkdir -p /user/root

wait_for_hdfs() {
    for _ in $(seq 1 30); do
        if hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes (1):"; then
            return 0
        fi
        sleep 2
    done
    echo "HDFS did not become ready in time"
    exit 1
}

wait_for_yarn() {
    for _ in $(seq 1 30); do
        if yarn node -list 2>/dev/null | grep -q "Total Nodes:.*1"; then
            return 0
        fi
        sleep 2
    done
    echo "YARN did not become ready in time"
    exit 1
}

wait_for_hdfs
wait_for_yarn
