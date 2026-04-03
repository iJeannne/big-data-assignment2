#!/bin/bash
set -euo pipefail

echo "[app] starting services"
service ssh restart
bash start-services.sh

echo "[app] preparing data"
bash prepare_data.sh

echo "[app] building index"
bash index.sh

echo "[app] running sample searches"
bash search.sh "history money banking"
