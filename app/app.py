import argparse
import subprocess
import socket
import time

from cassandra.cluster import Cluster
from cassandra import OperationTimedOut
from cassandra.cluster import NoHostAvailable


KEYSPACE = "search_engine"


def parse_args():
    parser = argparse.ArgumentParser(description="Load index data from HDFS into Cassandra.")
    parser.add_argument("--postings", default="/indexer/postings/part-00000")
    parser.add_argument("--vocabulary", default="/indexer/vocabulary/part-00000")
    parser.add_argument("--documents", default="/indexer/documents/part-00000")
    parser.add_argument("--stats", default="/indexer/stats/part-00000")
    parser.add_argument("--host", default="cassandra-server")
    parser.add_argument("--port", type=int, default=9042)
    return parser.parse_args()


def wait_for_tcp(host, port, retries=60, delay=5):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            with socket.create_connection((host, port), timeout=5):
                return
        except OSError as exc:
            last_error = exc
            print(f"Waiting for Cassandra TCP port {host}:{port} (attempt {attempt}/{retries})")
            time.sleep(delay)
    raise RuntimeError(f"Could not reach Cassandra TCP port {host}:{port}: {last_error}")


def wait_for_cassandra(host, port, retries=60, delay=5):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            cluster = Cluster([host], port=port, connect_timeout=10)
            session = cluster.connect()
            return cluster, session
        except (NoHostAvailable, OperationTimedOut, OSError) as exc:
            last_error = exc
            print(f"Waiting for Cassandra CQL {host}:{port} (attempt {attempt}/{retries})")
            time.sleep(delay)
        except Exception as exc:
            last_error = exc
            print(f"Waiting for Cassandra CQL {host}:{port} (attempt {attempt}/{retries})")
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Cassandra at {host}: {last_error}")


def read_hdfs_lines(path):
    result = subprocess.run(
        ["hdfs", "dfs", "-cat", path],
        check=True,
        capture_output=True,
        text=True,
    )
    return [line for line in result.stdout.splitlines() if line.strip()]


def create_schema(session):
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS search_engine
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
    )
    session.set_keyspace(KEYSPACE)

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
            doc_id text PRIMARY KEY,
            title text,
            doc_length int
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            df int
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS postings (
            term text,
            doc_id text,
            tf int,
            title text,
            doc_length int,
            PRIMARY KEY (term, doc_id)
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS corpus_stats (
            stat_name text PRIMARY KEY,
            stat_value double
        )
        """
    )


def clear_tables(session):
    for table in ("documents", "vocabulary", "postings", "corpus_stats"):
        session.execute(f"TRUNCATE {table}")


def load_documents(session, path):
    statement = session.prepare(
        "INSERT INTO documents (doc_id, title, doc_length) VALUES (?, ?, ?)"
    )
    for line in read_hdfs_lines(path):
        doc_id, title, doc_length = line.split("\t", 2)
        session.execute(statement, (doc_id, title, int(doc_length)))


def load_vocabulary(session, path):
    statement = session.prepare("INSERT INTO vocabulary (term, df) VALUES (?, ?)")
    for line in read_hdfs_lines(path):
        term, df = line.split("\t", 1)
        session.execute(statement, (term, int(df)))


def load_postings(session, path):
    statement = session.prepare(
        """
        INSERT INTO postings (term, doc_id, tf, title, doc_length)
        VALUES (?, ?, ?, ?, ?)
        """
    )
    for line in read_hdfs_lines(path):
        term, doc_id, tf, title, doc_length = line.split("\t", 4)
        session.execute(statement, (term, doc_id, int(tf), title, int(doc_length)))


def load_stats(session, path):
    statement = session.prepare(
        "INSERT INTO corpus_stats (stat_name, stat_value) VALUES (?, ?)"
    )
    for line in read_hdfs_lines(path):
        name, value = line.split("\t", 1)
        session.execute(statement, (name, float(value)))


def main():
    args = parse_args()
    wait_for_tcp(args.host, args.port)
    cluster, session = wait_for_cassandra(args.host, args.port)

    try:
        create_schema(session)
        clear_tables(session)
        load_documents(session, args.documents)
        load_vocabulary(session, args.vocabulary)
        load_postings(session, args.postings)
        load_stats(session, args.stats)
        print("Index data loaded into Cassandra keyspace search_engine.")
    finally:
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()
