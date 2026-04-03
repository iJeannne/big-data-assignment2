#!/bin/bash
set -euo pipefail

INPUT_PATH="${1:-/input/data}"

echo "Running index workflow for input path: $INPUT_PATH"
bash create_index.sh "$INPUT_PATH"
bash store_index.sh
