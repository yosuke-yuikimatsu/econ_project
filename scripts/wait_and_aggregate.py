from __future__ import annotations

import time

from app.storage.state import StateStore
from app.tasks.aggregate import build_final_json


if __name__ == "__main__":
    store = StateStore()
    idle_rounds = 0
    last = (-1, -1)

    while idle_rounds < 6:
        s = store.summary()
        fetched = s["metrics"].get("report_pages_fetched", 0)
        parsed = s["metrics"].get("report_pages_parsed", 0)
        cur = (fetched, parsed)
        print({"fetched": fetched, "parsed": parsed})
        if cur == last:
            idle_rounds += 1
        else:
            idle_rounds = 0
        last = cur
        time.sleep(10)

    result = build_final_json.delay()
    print({"aggregate_task_id": result.id})
