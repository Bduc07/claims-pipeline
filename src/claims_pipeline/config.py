"""Shared paths for the command-line runner and Airflow DAG."""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    root: Path

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        return cls(Path(os.environ.get("CLAIMS_PIPELINE_DATA", "data")).resolve())

    @property
    def landing(self) -> Path:
        return self.root / "landing"

    @property
    def output(self) -> Path:
        return self.root / "output"

    def rule_file(self, name: str) -> Path:
        override = self.root / "config" / name
        return override if override.is_file() else Path(__file__).with_name(name)

    def expected_files(self) -> tuple[str, ...]:
        manifest = self.root / "expected_files.txt"
        if not manifest.is_file():
            return ()
        return tuple(
            line.strip() for line in manifest.read_text(encoding="utf-8").splitlines() if line.strip()
        )
