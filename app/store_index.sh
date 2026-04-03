#!/bin/bash
set -euo pipefail

echo "Store the index in Cassandra"
python3 app.py \
    --postings /indexer/postings/part-00000 \
    --vocabulary /indexer/vocabulary/part-00000 \
    --documents /indexer/documents/part-00000 \
    --stats /indexer/stats/part-00000 \
    --host cassandra-server \
    --port 9042
