#!/usr/bin/env python3
import sys
import traceback


def flush_term(term, postings):
    if term is None or not postings:
        return
    for doc_id, tf, title, doc_length in postings:
        print(f"POSTING\t{term}\t{doc_id}\t{tf}\t{title}\t{doc_length}")
    print(f"VOCAB\t{term}\t{len(postings)}")


def main():
    current_term = None
    postings = []

    try:
        for line in sys.stdin:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue

            key = parts[0]
            marker = parts[1]

            if marker == "DOC" and len(parts) >= 5:
                print(f"DOC\t{parts[2]}\t{parts[3]}\t{parts[4]}")
                continue

            if marker != "POSTING" or len(parts) < 6:
                continue

            term = key
            if current_term is None:
                current_term = term

            if term != current_term:
                flush_term(current_term, postings)
                current_term = term
                postings = []

            postings.append((parts[2], parts[3], parts[4], parts[5]))

        flush_term(current_term, postings)
    except Exception as exc:  # pragma: no cover - streaming diagnostics
        sys.stderr.write(f"reducer1 failed: {exc}\n")
        traceback.print_exc(file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
