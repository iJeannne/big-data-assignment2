#!/usr/bin/env python3
import os
import sys
import traceback

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
for candidate in (CURRENT_DIR, PARENT_DIR):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from engine_utils import count_terms, parse_input_record


def main():
    for line_no, line in enumerate(sys.stdin, start=1):
        try:
            record = parse_input_record(line)
            if record is None:
                continue

            doc_id, title, text = record
            term_counts, doc_length = count_terms(text)
            if doc_length == 0:
                continue

            safe_title = title.replace("\t", " ").strip()
            print(f"__DOC__{doc_id}\tDOC\t{doc_id}\t{safe_title}\t{doc_length}")

            for term, tf in sorted(term_counts.items()):
                print(f"{term}\tPOSTING\t{doc_id}\t{tf}\t{safe_title}\t{doc_length}")
        except Exception as exc:  # pragma: no cover - streaming diagnostics
            sys.stderr.write(f"mapper1 failed on line {line_no}: {exc}\n")
            sys.stderr.write(line[:500] + "\n")
            traceback.print_exc(file=sys.stderr)
            raise


if __name__ == "__main__":
    main()
