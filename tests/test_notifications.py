import json
from pathlib import Path

from src.claims_pipeline.notifications import notify_failure


def test_failure_records_local_alert(tmp_path: Path) -> None:
    alert = notify_failure(tmp_path, "claims_pipeline", "manual__demo")
    event = json.loads(alert.read_text(encoding="utf-8"))
    assert alert.parent == tmp_path / "alerts"
    assert event["dag_id"] == "claims_pipeline"
    assert event["run_id"] == "manual__demo"
    assert event["status"] == "failed"
