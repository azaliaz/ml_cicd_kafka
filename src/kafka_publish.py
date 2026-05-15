"""Отправка результата предсказания в Kafka (Producer)."""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from typing import Any

from kafka_settings import KafkaSettings, load_kafka_settings
from logger import Logger, show_logs_from_env

_LOG = Logger(show_logs_from_env()).get_logger(__name__)

_producer_lock = threading.Lock()
_producer: Any = None
_producer_config: KafkaSettings | None = None


def _kafka_disabled() -> bool:
    return os.getenv("KAFKA_PUBLISH_DISABLED", "").lower() in ("1", "true", "yes")


def _get_producer(settings: KafkaSettings) -> Any:
    from kafka import KafkaProducer

    global _producer, _producer_config
    with _producer_lock:
        if _producer is not None and _producer_config == settings:
            return _producer
        if _producer is not None:
            try:
                _producer.flush(timeout=5)
                _producer.close(timeout=5)
            except Exception:
                _LOG.exception("kafka producer close")
            _producer = None
        _producer = KafkaProducer(
            bootstrap_servers=[h.strip() for h in settings.bootstrap_servers.split(",") if h.strip()],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            linger_ms=20,
            api_version_auto_timeout_ms=10000,
        )
        _producer_config = settings
        return _producer


def publish_prediction_event(
    *,
    prediction_id: int,
    median_house_value: float,
    features: dict[str, float],
    model_path: str | None = None,
) -> None:
    from kafka.errors import KafkaError

    if _kafka_disabled():
        return
    settings = load_kafka_settings()
    if settings is None:
        _LOG.debug("kafka: настройки не заданы (Vault/env), публикация пропущена")
        return
    payload: dict[str, Any] = {
        "event": "prediction",
        "prediction_id": prediction_id,
        "median_house_value": float(median_house_value),
        "features": features,
        "model_path": model_path,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    try:
        producer = _get_producer(settings)
        future = producer.send(settings.topic, value=payload)
        future.get(timeout=15)
        _LOG.info("kafka: отправлено в topic=%s prediction_id=%s", settings.topic, prediction_id)
    except KafkaError:
        _LOG.exception("kafka: ошибка отправки prediction_id=%s", prediction_id)
    except Exception:
        _LOG.exception("kafka: неожиданная ошибка prediction_id=%s", prediction_id)
