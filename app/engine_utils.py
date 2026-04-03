import math
import os
import re
from collections import Counter


TOKEN_PATTERN = re.compile(r"[A-Za-z0-9]+")


def tokenize(text):
    if not text:
        return []
    return TOKEN_PATTERN.findall(text.lower())


def count_terms(text):
    tokens = tokenize(text)
    return Counter(tokens), len(tokens)


def sanitize_title_from_path(path):
    filename = os.path.basename(path)
    stem, _ = os.path.splitext(filename)
    if "_" not in stem:
        return stem, stem
    doc_id, raw_title = stem.split("_", 1)
    return doc_id, raw_title.replace("_", " ")


def parse_input_record(line):
    parts = line.rstrip("\n").split("\t", 2)
    if len(parts) != 3:
        return None
    return parts[0], parts[1], parts[2]


def bm25_score(tf, df, doc_length, total_docs, avg_doc_length, k1=1.2, b=0.75):
    if tf <= 0 or df <= 0 or total_docs <= 0 or avg_doc_length <= 0:
        return 0.0
    idf = math.log(total_docs / df)
    norm = k1 * ((1 - b) + b * (doc_length / avg_doc_length))
    return idf * ((tf * (k1 + 1)) / (tf + norm))
