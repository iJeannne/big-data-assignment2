FROM firasj/spark-docker-cluster

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip python3-setuptools python3-wheel \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel \
    && python3 -m pip install --no-cache-dir pyspark==3.5.4 cassandra-driver pathvalidate

ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYSPARK_PYTHON=python3
