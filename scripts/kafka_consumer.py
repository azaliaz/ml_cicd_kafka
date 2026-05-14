#!/usr/bin/env python3
"""Kafka Consumer: читает сообщения с результатом предсказания (демо-сервис для compose/CD)."""
from __future__ import annotations

import json
import os
import signal
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from kafka import KafkaConsumer  # noqa: E402
from kafka_settings import load_kafka_settings  # noqa: E402
from logger import Logger, show_logs_from_env  # noqa: E402

_LOG = Logger(show_logs_from_env()).get_logger(__name__)
_stop = False


def _handle_sig(*_args: object) -> None:
    global _stop
    _stop = True


def main() -> int:
    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    settings = load_kafka_settings()
    if settings is None:
        _LOG.error("Не заданы Kafka настройки (KAFKA_* или Vault VAULT_KAFKA_SECRET_PATH)")
        return 2

    servers = [h.strip() for h in settings.bootstrap_servers.split(",") if h.strip()]
    consumer = KafkaConsumer(
        settings.topic,
        bootstrap_servers=servers,
        auto_offset_reset=os.getenv("KAFKA_CONSUMER_AUTO_OFFSET_RESET", "earliest"),
        enable_auto_commit=True,
        group_id=os.getenv("KAFKA_CONSUMER_GROUP_ID", "housing-consumer"),
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        api_version_auto_timeout_ms=15000,
    )
    _LOG.info("consumer: подписка topic=%s bootstrap=%s", settings.topic, servers)
    try:
        for msg in consumer:
            if _stop:
                break
            _LOG.info("consumer: partition=%s offset=%s value=%s", msg.partition, msg.offset, msg.value)
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
