"""Checkpoint manager for recovery."""

import pickle
from datetime import datetime
from pathlib import Path

from .models import CheckpointData


class CheckpointManager:
    """Saves and restores pipeline state for resume."""

    def __init__(self, checkpoint_dir: Path, enabled: bool = True):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.enabled = enabled
        if self.enabled:
            self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def save(
        self,
        phase: str,
        papers: list[dict],
        papers_with_abstracts: int,
        custom_data: dict | None = None,
    ) -> Path:
        if not self.enabled:
            return Path()

        filepath = self.checkpoint_dir / self._filename(phase)
        checkpoint = CheckpointData(
            phase=phase,
            papers_processed=len(papers),
            papers_with_abstracts=papers_with_abstracts,
            custom_data=custom_data or {},
        )
        with open(filepath, "wb") as fh:
            pickle.dump({"checkpoint": checkpoint.model_dump(), "papers": papers}, fh)

        self._prune_old(phase)
        return filepath

    def load(self, specific_file: Path | None = None) -> tuple[CheckpointData, list[dict]] | None:
        if not self.enabled:
            return None

        filepath = specific_file or self._latest()
        if filepath is None or not filepath.exists():
            return None

        with open(filepath, "rb") as fh:
            data = pickle.load(fh)

        return CheckpointData(**data["checkpoint"]), data.get("papers", [])

    def list_checkpoints(self) -> list[Path]:
        if not self.checkpoint_dir.exists():
            return []
        return sorted(self.checkpoint_dir.glob("checkpoint_*.pkl"), reverse=True)

    def clear(self) -> int:
        count = 0
        for cp in self.list_checkpoints():
            cp.unlink()
            count += 1
        return count

    def _filename(self, phase: str) -> str:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"checkpoint_{phase}_{ts}.pkl"

    def _latest(self) -> Path | None:
        checkpoints = self.list_checkpoints()
        return checkpoints[0] if checkpoints else None

    def _prune_old(self, phase: str, keep: int = 5) -> None:
        pattern = f"checkpoint_{phase}_*.pkl"
        for old in sorted(self.checkpoint_dir.glob(pattern), reverse=True)[keep:]:
            old.unlink()
