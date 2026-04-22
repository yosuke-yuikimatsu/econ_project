from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.tasks.aggregate import build_final_json
from app.tasks.bootstrap import start_bootstrap


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("bootstrap")
    sub.add_parser("aggregate")

    args = parser.parse_args()
    if args.cmd == "bootstrap":
        r = start_bootstrap.delay()
        print(json.dumps({"task_id": r.id, "queue": "bootstrap"}, ensure_ascii=False))
    elif args.cmd == "aggregate":
        r = build_final_json.delay()
        print(json.dumps({"task_id": r.id, "queue": "aggregate"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
