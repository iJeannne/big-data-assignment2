## app folder
This folder contains the data folder and all scripts and source code that are required to run your simple search engine. 

### data
This folder stores the text documents required to index. The pipeline uses a sample of 100 documents from `a.parquet` or from the bundled fallback corpus.

### mapreduce
This folder stores the mapper `mapperx.py` and reducer `reducerx.py` scripts for the MapReduce pipelines.

### app.py
This is a Python file to write code to store index data in Cassandra.

### app.sh
The entrypoint for the executables in your repository and includes all commands that will run your programs in this folder.

### create_index.sh
A script to create index data using MapReduce pipelines and store them in HDFS.

### index.sh
A script to run the MapReduce pipelines and the programs to store data in Cassandra/ScyllaDB.

### prepare_data.py
The script that will create documents from parquet file. You can run it in the driver.

### prepare_data.sh
The script that will run the prevoious Python file and will copy the data to HDFS.

### query.py
A Python file to write PySpark app that will process a user's query and retrieves a list of top 10 relevant documents ranked using BM25.

### requirements.txt
This file contains the Python dependencies that are baked into the shared Docker image for the master and worker nodes.

### search.sh
This script runs the `query.py` PySpark app on YARN in client mode from the master container.


### start-services.sh
This script initiates the Hadoop components. It uses the Hadoop XML files mounted by `docker-compose.yml` and is called in `app.sh`.


### store_index.sh
This script will create Cassandra/ScyllaDB tables and load the index data from HDFS to them.
