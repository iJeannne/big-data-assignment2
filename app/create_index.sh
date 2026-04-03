#!/bin/bash

set -euo pipefail

INPUT_PATH="${1:-/input/data}"
STREAMING_JAR=$(find "$HADOOP_HOME/share/hadoop/tools/lib" -maxdepth 1 -name 'hadoop-streaming-*.jar' ! -name '*sources*' | head -n 1)
TMP_DIR=$(mktemp -d)

cleanup() {
    rm -rf "$TMP_DIR"
}

trap cleanup EXIT

if [ -z "$STREAMING_JAR" ]; then
    echo "Unable to locate hadoop-streaming jar"
    exit 1
fi

echo "Create index using MapReduce pipelines"
echo "Input path: $INPUT_PATH"

hdfs dfs -test -e "$INPUT_PATH"
hdfs dfs -rm -r -f /tmp/indexer >/dev/null 2>&1 || true
hdfs dfs -rm -r -f /indexer >/dev/null 2>&1 || true

echo "Running pipeline 1: postings, vocabulary, document stats"
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="search-engine-index-pipeline1" \
    -D mapreduce.job.reduces=1 \
    -cmdenv PYTHONUNBUFFERED=1 \
    -file mapreduce/mapper1.py \
    -file mapreduce/reducer1.py \
    -file engine_utils.py \
    -mapper "python3 -u mapper1.py" \
    -reducer "python3 -u reducer1.py" \
    -input "$INPUT_PATH" \
    -output /tmp/indexer/pipeline1

echo "Running pipeline 2: corpus statistics"
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="search-engine-index-pipeline2" \
    -D mapreduce.job.reduces=1 \
    -cmdenv PYTHONUNBUFFERED=1 \
    -file mapreduce/mapper2.py \
    -file mapreduce/reducer2.py \
    -mapper "python3 -u mapper2.py" \
    -reducer "python3 -u reducer2.py" \
    -input /tmp/indexer/pipeline1 \
    -output /tmp/indexer/pipeline2

echo "Splitting pipeline outputs into final HDFS folders"
hdfs dfs -cat /tmp/indexer/pipeline1/part-* > "$TMP_DIR/pipeline1.tsv"
hdfs dfs -cat /tmp/indexer/pipeline2/part-* > "$TMP_DIR/pipeline2.tsv"

awk -F '\t' 'BEGIN{OFS="\t"} $1=="POSTING"{print $2,$3,$4,$5,$6}' "$TMP_DIR/pipeline1.tsv" > "$TMP_DIR/postings.tsv"
awk -F '\t' 'BEGIN{OFS="\t"} $1=="VOCAB"{print $2,$3}' "$TMP_DIR/pipeline1.tsv" > "$TMP_DIR/vocabulary.tsv"
awk -F '\t' 'BEGIN{OFS="\t"} $1=="DOC"{print $2,$3,$4}' "$TMP_DIR/pipeline1.tsv" > "$TMP_DIR/documents.tsv"
awk -F '\t' 'BEGIN{OFS="\t"} $1=="STAT"{print $2,$3}' "$TMP_DIR/pipeline2.tsv" > "$TMP_DIR/stats.tsv"

hdfs dfs -mkdir -p /indexer/postings /indexer/vocabulary /indexer/documents /indexer/stats
hdfs dfs -put -f "$TMP_DIR/postings.tsv" /indexer/postings/part-00000
hdfs dfs -put -f "$TMP_DIR/vocabulary.tsv" /indexer/vocabulary/part-00000
hdfs dfs -put -f "$TMP_DIR/documents.tsv" /indexer/documents/part-00000
hdfs dfs -put -f "$TMP_DIR/stats.tsv" /indexer/stats/part-00000

echo "Index created in HDFS:"
hdfs dfs -ls /indexer
