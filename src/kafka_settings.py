"""Параметры Kafka: из Vault (KV v2) или из переменных окружения."""
from __future__ import annotations

import os
from dataclasses import dataclass

from vault_kv import read_kv_v2_secret


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    topic: str


def _vault_kafka_kv_path() -> str:
    return os.getenv("VAULT_KAFKA_SECRET_PATH", "secret/data/housing/kafka").lstrip("/")


def load_kafka_settings() -> KafkaSettings | None:
    bootstrap = (os.getenv("KAFKA_BOOTSTRAP_SERVERS") or "").strip()
    topic = (os.getenv("KAFKA_TOPIC") or "").strip()
    if bootstrap and topic:
        return KafkaSettings(bootstrap_servers=bootstrap, topic=topic)

    addr = (os.getenv("VAULT_ADDR") or "").rstrip("/")
    token = (os.getenv("VAULT_TOKEN") or "").strip()
    if not addr or not token:
        return None
    data = read_kv_v2_secret(addr=addr, token=token, secret_path=_vault_kafka_kv_path())
    bs = str(data.get("bootstrap_servers") or "").strip()
    t = str(data.get("topic") or "").strip()
    if not bs or not t:
        return None
    return KafkaSettings(bootstrap_servers=bs, topic=t)
