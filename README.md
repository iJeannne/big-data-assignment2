# big-data-assignment2

## Run
```bash
docker compose up
```

The master container runs `app/app.sh`, which:
- starts Hadoop services
- prepares the dataset
- uses 100 documents from the available corpus
- builds the index with Hadoop MapReduce
- stores the index in Cassandra
- runs a sample BM25 search on Spark running in YARN client mode

## Project layout
- `Dockerfile` builds the shared cluster image with the Python dependencies
- `app/prepare_data.py` prepares the corpus and HDFS input
- `app/mapreduce/mapper1.py` and `app/mapreduce/reducer1.py` build postings, vocabulary, and document stats
- `app/mapreduce/mapper2.py` and `app/mapreduce/reducer2.py` build corpus statistics
- `app/create_index.sh` runs the MapReduce pipelines
- `app/store_index.sh` loads the index into Cassandra
- `app/query.py` ranks documents with BM25
- `app/search.sh` runs the query job with Spark on YARN in client mode
- `config/hadoop/*.xml` provides the shared Hadoop and YARN configuration for both nodes
