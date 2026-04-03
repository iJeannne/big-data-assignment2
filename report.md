# Big Data Assignment 2 Report

## 1. Goal

The goal of this assignment was to build a simple search engine for text documents in a Hadoop cluster.

The system must:

- prepare data with PySpark
- use a Hadoop cluster with a master and a slave
- build an index with Hadoop MapReduce
- store the index in Cassandra
- run search queries with BM25 ranking

I used only distributed tools for the data pipeline. I did not use pandas or any other single-machine data processing package.

## 2. Cluster Setup

The project runs with Docker Compose.

The cluster contains:

- `cluster-master`
- `cluster-slave-1`
- `cassandra-server`

The master container starts the whole workflow. It runs the setup scripts, prepares the data, builds the index, loads the index into Cassandra, and runs sample searches.

The slave container is used by Hadoop services as the worker node.

The Cassandra container stores the final index data.

## 3. Project Idea

The assignment is a small search engine for text documents.

The workflow is:

1. Prepare the corpus.
2. Copy the documents to HDFS.
3. Build the index with Hadoop MapReduce.
4. Store the index in Cassandra.
5. Run BM25 search queries.

The search engine works with 100 documents. This matches the instructor note and it also makes the system lighter and more stable for Docker Desktop.

## 4. Data Preparation

Data preparation is done with PySpark in `prepare_data.py`.

This part is important because the assignment says that data preparation must use PySpark. I followed that rule.

What this step does:

- reads the source corpus
- selects 100 text documents
- creates local text files in `app/data`
- writes the prepared data into HDFS

The output of this step is visible in the logs:

- `Prepared HDFS paths:`
- `Found 100 items`

This shows that the project uses exactly 100 documents.

## 5. Index Building

Index building is done with Hadoop MapReduce in `create_index.sh`.

The indexing pipeline has two MapReduce jobs.

### Pipeline 1

Pipeline 1 creates:

- postings
- vocabulary
- document statistics

The first mapper reads each document and emits term information.
The first reducer groups terms and builds the postings and vocabulary data.

### Pipeline 2

Pipeline 2 creates corpus statistics.

It computes:

- total number of documents
- average document length

These values are needed later for BM25 scoring.

The output of the indexing step is stored in HDFS under:

- `/indexer/documents`
- `/indexer/postings`
- `/indexer/vocabulary`
- `/indexer/stats`

The logs confirm success with:

- `Index created in HDFS:`

## 6. Cassandra Storage

After the index is created in HDFS, the project loads the data into Cassandra.

This step is done by `app.py` through `store_index.sh`.

The Cassandra keyspace is:

- `search_engine`

The tables store:

- documents
- vocabulary
- postings
- corpus statistics

The logs confirm success with:

- `Index data loaded into Cassandra keyspace search_engine.`

This is the final storage layer for the index.

## 7. Search Engine

The query engine is implemented in `query.py`.

It uses:

- PySpark
- Cassandra
- BM25 ranking

How it works:

1. The query text is read from the command line.
2. The query is tokenized.
3. The app reads the needed rows from Cassandra.
4. BM25 score is calculated for each document.
5. The best 10 documents are returned.

The output is a list of document IDs and titles.

The logs show:

- `Searching for documents using BM25`
- `Job 0 finished: takeOrdered ...`
- 10 result lines

This proves that the search engine works.

## 8. Important Technical Choices

I made several practical choices to keep the project stable:

- I used 100 documents instead of 1000, as required by the instructor.
- I used PySpark for preparation and search logic.
- I used Hadoop MapReduce for the indexing stage.
- I used Cassandra to store the final index.
- I kept the Spark search job in YARN client mode, and I moved Python dependencies into the Docker image plus Hadoop config into mounted XML files to make the startup more stable.
- I used the same custom Docker image for both Hadoop nodes, so the worker and master run with the same Python packages.

This choice is important. The BM25 logic itself works. The YARN client-mode search job now starts from a cleaner container image and a shared Hadoop configuration, which reduces the startup problems from the earlier prototype.

## 9. Validation

I checked the project with a clean clone and a full Docker run.

The expected workflow is:

```bash
docker compose up
```

The final successful run shows:

- 100 documents prepared
- the index built in HDFS
- the index loaded into Cassandra
- BM25 search results printed in the terminal

The final search output ends with:

- `SparkContext is stopping with exitCode 0.`
- `cluster-master exited with code 0`

This means the complete pipeline finished successfully.

## 10. Screenshots

### 10.1 Successful Indexing of 100 Documents

Paste your first screenshot here.

It should show:

- `Prepared HDFS paths:`
- `Found 100 items`
- `Index created in HDFS:`
- `Index data loaded into Cassandra keyspace search_engine.`

### 10.2 Successful Search Execution

Paste your second screenshot here.

It should show:

- `Searching for documents using BM25`
- `Job 0 finished: takeOrdered ...`
- 10 result lines with document IDs and titles
- `SparkContext is stopping with exitCode 0.`

## 11. Conclusion

The assignment is completed successfully.

The project now has a full search pipeline:

- data preparation with PySpark
- indexing with Hadoop MapReduce
- storage in Cassandra
- document ranking with BM25

The system runs inside Docker Compose and starts from `docker compose up`.

The final result is a working search engine for 100 documents, with stable output and reproducible execution.
