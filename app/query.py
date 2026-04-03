import sys

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

from engine_utils import bm25_score, tokenize


KEYSPACE = "search_engine"


def read_query():
    if len(sys.argv) > 1:
        return " ".join(sys.argv[1:]).strip()
    return sys.stdin.read().strip()


def load_stats(session):
    rows = session.execute("SELECT stat_name, stat_value FROM corpus_stats")
    stats = {row.stat_name: row.stat_value for row in rows}
    return int(stats.get("N", 0)), float(stats.get("AVGDL", 0.0))


def main():
    query = read_query()
    terms = tokenize(query)
    if not terms:
        print("No query terms provided.")
        return

    spark = SparkSession.builder.appName("bm25-query").getOrCreate()
    cluster = Cluster(["cassandra-server"])
    session = cluster.connect(KEYSPACE)

    try:
        total_docs, avg_doc_length = load_stats(session)
        if total_docs == 0 or avg_doc_length == 0:
            print("Index statistics are missing.")
            return

        vocab_stmt = session.prepare("SELECT df FROM vocabulary WHERE term = ?")
        postings_stmt = session.prepare(
            "SELECT term, doc_id, tf, title, doc_length FROM postings WHERE term = ?"
        )

        scored_inputs = []
        for term in terms:
            vocab_row = session.execute(vocab_stmt, (term,)).one()
            if vocab_row is None or vocab_row.df <= 0:
                continue
            df = int(vocab_row.df)
            for row in session.execute(postings_stmt, (term,)):
                scored_inputs.append((row.doc_id, row.title, row.tf, row.doc_length, df))

        if not scored_inputs:
            print("No matching documents found.")
            return

        results = (
            spark.sparkContext.parallelize(scored_inputs)
            .map(
                lambda item: (
                    (item[0], item[1]),
                    bm25_score(item[2], item[4], item[3], total_docs, avg_doc_length),
                )
            )
            .reduceByKey(lambda left, right: left + right)
            .map(lambda item: (item[0][0], item[0][1], item[1]))
            .takeOrdered(10, key=lambda item: -item[2])
        )

        for doc_id, title, _score in results:
            print(f"{doc_id}\t{title}")
    finally:
        session.shutdown()
        cluster.shutdown()
        spark.stop()


if __name__ == "__main__":
    main()
