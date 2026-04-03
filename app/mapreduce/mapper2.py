#!/usr/bin/env python3
import sys
import traceback


def main():
    try:
        for line in sys.stdin:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 4 or parts[0] != "DOC":
                continue
            print(f"__STATS__\t1\t{parts[3]}")
    except Exception as exc:  # pragma: no cover - streaming diagnostics
        sys.stderr.write(f"mapper2 failed: {exc}\n")
        traceback.print_exc(file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
