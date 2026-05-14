"""Настройки Kafka без поднятого кластера."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

import kafka_settings  # noqa: E402


def test_load_kafka_settings_prefers_explicit_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "t1")
    s = kafka_settings.load_kafka_settings()
    assert s is not None
    assert s.bootstrap_servers == "localhost:9092"
    assert s.topic == "t1"


def test_load_kafka_settings_none_without_config(monkeypatch):
    for k in (
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_TOPIC",
        "VAULT_ADDR",
        "VAULT_TOKEN",
        "VAULT_KAFKA_SECRET_PATH",
    ):
        monkeypatch.delenv(k, raising=False)
    assert kafka_settings.load_kafka_settings() is None
