"""Record a failed Airflow run and optionally notify a webhook."""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import Request, urlopen
from uuid import uuid4

logger = logging.getLogger(__name__)


def notify_failure(output: Path, dag_id: str, run_id: str, webhook: str | None = None) -> Path:
    alerts = output / "alerts"
    alerts.mkdir(parents=True, exist_ok=True)
    alert = alerts / f"{uuid4()}.json"
    event = {
        "dag_id": dag_id,
        "run_id": run_id,
        "status": "failed",
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    alert.write_text(json.dumps(event) + "\n", encoding="utf-8")
    logger.error("Airflow DAG %s failed for run %s. Alert recorded at %s", dag_id, run_id, alert)
    if webhook:
        message = {"text": f"Airflow DAG {dag_id} failed for run {run_id}. See Airflow task logs."}
        request = Request(
            webhook,
            data=json.dumps(message).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=5):
                pass
        except OSError:
            logger.error("Webhook notification failed. The local alert is saved at %s", alert)
    return alert
