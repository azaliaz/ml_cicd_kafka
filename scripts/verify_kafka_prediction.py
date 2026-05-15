#!/usr/bin/env python3
"""Проверка, что после вызова /predict в Kafka есть сообщение с prediction_id (этап CD Jenkins)."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from kafka import KafkaConsumer  # noqa: E402
from kafka_settings import load_kafka_settings  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=float, default=90.0)
    parser.add_argument("--report-json", type=Path, default=None)
    args = parser.parse_args()

    settings = load_kafka_settings()
    if settings is None:
        print("Kafka settings missing", file=sys.stderr)
        return 2

    servers = [h.strip() for h in settings.bootstrap_servers.split(",") if h.strip()]
    consumer = KafkaConsumer(
        settings.topic,
        bootstrap_servers=servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=f"verify-{int(time.time())}",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        api_version_auto_timeout_ms=15000,
    )

    deadline = time.monotonic() + args.timeout
    found: dict | None = None
    err: str | None = None
    try:
        while time.monotonic() < deadline and found is None:
            polled = consumer.poll(timeout_ms=2000)
            for _tp, records in polled.items():
                for rec in records:
                    val = rec.value
                    if isinstance(val, dict) and val.get("event") == "prediction" and val.get("prediction_id") is not None:
                        found = val
                        break
                if found is not None:
                    break
    except Exception as e:
        err = str(e)
    finally:
        consumer.close()

    ok = found is not None
    report = {
        "ok": ok,
        "topic": settings.topic,
        "timeout_sec": args.timeout,
        "sample_message": found,
        "error": err,
    }
    if args.report_json is not None:
        args.report_json.parent.mkdir(parents=True, exist_ok=True)
        args.report_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if not ok:
        print(json.dumps(report, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
