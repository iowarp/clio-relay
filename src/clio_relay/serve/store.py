"""Where relay keeps its service plans: one private JSON file per operation."""

from __future__ import annotations

import contextlib
import os
import tempfile
from pathlib import Path

from pydantic import ValidationError

from clio_relay.serve.plan import ServePlan


class PlanStore:
    """``<state>/serve/plans/<operation_id>.json``, readable by the owner only."""

    def __init__(self, state_dir: Path) -> None:
        self.root = state_dir / "serve" / "plans"

    def path(self, operation_id: str) -> Path:
        """Return the plan file of one operation."""
        return self.root / f"{operation_id}.json"

    def get(self, operation_id: str) -> ServePlan | None:
        """Read a plan, or ``None``."""
        try:
            text = self.path(operation_id).read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        try:
            return ServePlan.model_validate_json(text)
        except ValidationError:
            return None

    def put(self, plan: ServePlan) -> None:
        """Write a plan atomically, mode 600."""
        self.root.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(OSError):
            os.chmod(self.root, 0o700)
        fd, temp = tempfile.mkstemp(prefix=".plan.", dir=self.root)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(plan.model_dump_json(indent=2))
            with contextlib.suppress(OSError):
                os.chmod(temp, 0o600)
            os.replace(temp, self.path(plan.operation_id))
        except BaseException:
            Path(temp).unlink(missing_ok=True)
            raise

    def all(self) -> list[ServePlan]:
        """Return every plan."""
        if not self.root.is_dir():
            return []
        plans: list[ServePlan] = []
        for path in sorted(self.root.glob("*.json")):
            plan = self.get(path.stem)
            if plan is not None:
                plans.append(plan)
        return plans
