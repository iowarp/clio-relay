"""Package reviews, pinned by hash (RL24).

A review is the person's yes to one package archive on one site. It is kept at
``<state>/guard/reviews/<site>/<key>.json``, where the key is the digest of the
package's name, version and SHA-256, so a changed archive, a new version or a
new name is never covered by an old yes. Deleting the file withdraws the review.

Relay asks for a review through an approve ask whose binding has
``kind: package_review``. The guard turns an accepted one into a record the
next time it looks, so a review survives a restart and needs no hook into the
moment the person answers.
"""

from __future__ import annotations

import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from clio_relay.adapters.hooks import Ask
from clio_relay.jobs.handles import utc_now
from clio_relay.jobs.records import RecordStore
from clio_relay.jobs.spec import PackageRef, canonical_digest
from clio_relay.sites.models import SITE_ID_PATTERN

REVIEW_KIND = "package_review"
_SITE_ID = re.compile(SITE_ID_PATTERN)


class Review(BaseModel):
    """One reviewed package on one site."""

    site: str
    package: str
    version: str
    sha256: str
    source: dict[str, Any] | None = None
    ask_id: str | None = None
    answered_by: str | None = None
    reviewed_at: datetime = Field(default_factory=utc_now)


def review_key(package: str, version: str, sha256: str) -> str:
    """Return the key a review is filed under."""
    return canonical_digest({"package": package, "version": version, "sha256": sha256.lower()})


class ReviewStore:
    """The reviews this relay host has recorded."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, site: str, key: str) -> Path:
        if not _SITE_ID.match(site):
            raise ValueError(f"not a site id: {site!r}")
        return self.root / site / f"{key}.json"

    def get(self, site: str, package: PackageRef) -> Review | None:
        """Return the review of ``package`` on ``site``, if there is one."""
        path = self._path(site, review_key(package.name, package.version, package.sha256))
        try:
            review = Review.model_validate_json(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, ValidationError):
            return None
        same = (review.site, review.package, review.version, review.sha256) == (
            site,
            package.name,
            package.version,
            package.sha256,
        )
        return review if same else None

    def put(self, review: Review) -> None:
        """Record ``review``."""
        path = self._path(review.site, review_key(review.package, review.version, review.sha256))
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".review.", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(review.model_dump_json(indent=2))
            os.replace(tmp, path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def all(self, site: str | None = None) -> list[Review]:
        """Return every review, or one site's."""
        if not self.root.is_dir():
            return []
        folders = [self.root / site] if site is not None else sorted(self.root.iterdir())
        found: list[Review] = []
        for folder in folders:
            if not folder.is_dir():
                continue
            for path in sorted(folder.glob("*.json")):
                try:
                    found.append(Review.model_validate_json(path.read_text(encoding="utf-8")))
                except ValidationError:
                    continue
        return found

    def harvest(self, asks: RecordStore[Ask]) -> int:
        """Record every accepted, untampered package review ask. Returns how many were new."""
        added = 0
        for ask in asks.all():
            review = _review_of(ask)
            if review is None:
                continue
            package = PackageRef(name=review.package, version=review.version, sha256=review.sha256)
            if self.get(review.site, package) is not None:
                continue
            self.put(review)
            added += 1
        return added


def _review_of(ask: Ask) -> Review | None:
    binding = ask.binding
    if ask.kind != "approve" or ask.state != "accepted" or binding.get("kind") != REVIEW_KIND:
        return None
    if ask.binding_digest != canonical_digest(binding):
        return None
    try:
        package = PackageRef(
            name=str(binding["package"]),
            version=str(binding["version"]),
            sha256=str(binding["package_hash"]),
        )
    except (KeyError, ValidationError):
        return None
    site = str(binding.get("site") or ask.site)
    if site != ask.site or not _SITE_ID.match(site):
        return None
    source = binding.get("source")
    return Review(
        site=site,
        package=package.name,
        version=package.version,
        sha256=package.sha256,
        source=source if isinstance(source, dict) else None,
        ask_id=ask.ask_id,
        answered_by=ask.answered_by,
        reviewed_at=ask.answered_at or utc_now(),
    )
