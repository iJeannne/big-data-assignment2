#!/usr/bin/env python3
import sys
import traceback


def main():
    total_docs = 0
    total_length = 0

    try:
        for line in sys.stdin:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 3 or parts[0] != "__STATS__":
                continue
            total_docs += int(parts[1])
            total_length += int(parts[2])

        avgdl = (total_length / total_docs) if total_docs else 0.0
        print(f"STAT\tN\t{total_docs}")
        print(f"STAT\tAVGDL\t{avgdl:.6f}")
    except Exception as exc:  # pragma: no cover - streaming diagnostics
        sys.stderr.write(f"reducer2 failed: {exc}\n")
        traceback.print_exc(file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
