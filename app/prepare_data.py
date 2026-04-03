import glob
import os
import subprocess
import shutil

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from engine_utils import sanitize_title_from_path


APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_DIR, "data")
GENERATED_DIR = os.path.join(DATA_DIR, "generated")
LOCAL_PARQUET_PATH = os.path.join(APP_DIR, "a.parquet")
HDFS_PARQUET_PATH = "hdfs:///a.parquet"
INPUT_OUTPUT = "hdfs:///input/data"
HDFS_DATA_DIR = "/data"
LIMIT_DOCS = 100


def ensure_clean_directory(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def create_docs_from_parquet(spark, limit_docs=LIMIT_DOCS):
    parquet_exists = subprocess.run(
        ["hdfs", "dfs", "-test", "-e", "/a.parquet"],
        check=False,
    ).returncode == 0

    if not parquet_exists:
        if os.path.exists(GENERATED_DIR):
            shutil.rmtree(GENERATED_DIR)
        return False

    ensure_clean_directory(GENERATED_DIR)

    df = spark.read.parquet(HDFS_PARQUET_PATH)
    df = (
        df.select("id", "title", "text")
        .where(F.col("text").isNotNull())
        .where(F.length(F.trim(F.col("text"))) > 0)
    )

    total_docs = df.count()
    fraction = min(1.0, (limit_docs * 2) / max(total_docs, 1))
    sampled = df.sample(withReplacement=False, fraction=fraction, seed=0).limit(limit_docs)

    def create_doc(row):
        title = row["title"] or "untitled"
        filename = sanitize_filename(f"{row['id']}_{title}").replace(" ", "_") + ".txt"
        path = os.path.join(GENERATED_DIR, filename)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(row["text"])

    for row in sampled.toLocalIterator():
        create_doc(row)
    return True


def upload_documents_to_hdfs():
    source_dir = GENERATED_DIR if glob.glob(os.path.join(GENERATED_DIR, "*.txt")) else DATA_DIR
    files = sorted(glob.glob(os.path.join(source_dir, "*.txt")))[:LIMIT_DOCS]
    if not files:
        raise RuntimeError("No local text documents found in app/data.")

    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", HDFS_DATA_DIR], check=False)
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_DATA_DIR], check=True)
    subprocess.run(["hdfs", "dfs", "-put", "-f", *files, HDFS_DATA_DIR], check=True)


def build_input_from_hdfs_text_files(spark):
    rdd = spark.sparkContext.wholeTextFiles("hdfs:///data/*.txt")
    records = (
        rdd.map(lambda item: (sanitize_title_from_path(item[0]), item[1]))
        .map(lambda item: (item[0][0], item[0][1], item[1].replace("\n", " ").strip()))
        .filter(lambda item: item[0] and item[1] and item[2])
        .map(lambda item: f"{item[0]}\t{item[1]}\t{item[2]}")
        .coalesce(1)
    )
    records.saveAsTextFile(INPUT_OUTPUT)


def main():
    spark = (
        SparkSession.builder.appName("data preparation")
        .master("local")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate()
    )

    create_docs_from_parquet(spark)
    upload_documents_to_hdfs()
    build_input_from_hdfs_text_files(spark)
    spark.stop()


if __name__ == "__main__":
    main()
