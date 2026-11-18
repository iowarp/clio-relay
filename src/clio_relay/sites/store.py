"""The site store over ``sites.yaml``. The command line and the compute tab both call it."""

from __future__ import annotations

import os
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

import yaml
from filelock import FileLock
from pydantic import ValidationError

from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import CatalogEntry, SiteEntry


class SiteStore:
    """Reads and writes the person's ``sites.yaml``, a top-level list of entries."""

    def __init__(self, path: Path) -> None:
        self.path = path

    @contextmanager
    def _locked(self) -> Generator[None]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(str(self.path) + ".lock", timeout=30):
            yield

    def _read(self) -> list[SiteEntry]:
        try:
            text = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return []
        loaded: object = yaml.safe_load(text)
        if loaded is None:
            return []
        if not isinstance(loaded, list):
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                f"{self.path} must be a list of site entries",
                path=str(self.path),
            )
        entries: list[SiteEntry] = []
        for index, item in enumerate(cast(list[object], loaded)):
            try:
                entries.append(SiteEntry.model_validate(item))
            except ValidationError as exc:
                raise Refusal(
                    RefusalReason.INVALID_CONFIG,
                    f"entry {index} of {self.path} is not a valid site",
                    path=str(self.path),
                    index=index,
                    errors=exc.errors(include_url=False, include_context=False),
                ) from exc
        ids = [entry.id for entry in entries]
        duplicates = sorted({site for site in ids if ids.count(site) > 1})
        if duplicates:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                f"{self.path} names a site more than once: {', '.join(duplicates)}",
                path=str(self.path),
            )
        return entries

    def _write(self, entries: list[SiteEntry]) -> None:
        data = [entry.model_dump(mode="json", exclude_defaults=True) for entry in entries]
        for item, entry in zip(data, entries, strict=True):
            item["id"] = entry.id
            item["adapter"] = entry.adapter
            item["policy"] = entry.policy
        text = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)
        fd, tmp = tempfile.mkstemp(prefix=".sites.", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(text)
            os.replace(tmp, self.path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def list(self) -> list[SiteEntry]:
        """Return every site, in file order."""
        return self._read()

    def get(self, site_id: str) -> SiteEntry:
        """Return one site. Refuses with ``site_not_found``."""
        for entry in self._read():
            if entry.id == site_id:
                return entry
        raise Refusal(
            RefusalReason.SITE_NOT_FOUND,
            f"no site is named {site_id!r}",
            site=site_id,
            known=[entry.id for entry in self._read()],
        )

    def add(self, entry: SiteEntry) -> SiteEntry:
        """Add a site. Refuses with ``site_exists``."""
        with self._locked():
            entries = self._read()
            if any(item.id == entry.id for item in entries):
                raise Refusal(
                    RefusalReason.SITE_EXISTS, f"a site named {entry.id!r} exists", site=entry.id
                )
            entries.append(entry)
            self._write(entries)
        return entry

    def add_from_catalog(self, catalog: CatalogEntry, site_id: str | None = None) -> SiteEntry:
        """Copy a catalog entry into ``sites.yaml``, where the person edits it."""
        return self.add(catalog.to_site(site_id))

    def edit(self, site_id: str, changes: dict[str, Any]) -> SiteEntry:
        """Apply top-level field changes to a site and validate the result."""
        if "id" in changes and changes["id"] != site_id:
            raise Refusal(
                RefusalReason.INVALID_ARGUMENTS, "a site's id cannot change", site=site_id
            )
        with self._locked():
            entries = self._read()
            for index, entry in enumerate(entries):
                if entry.id != site_id:
                    continue
                merged = entry.model_dump(mode="json") | changes
                try:
                    updated = SiteEntry.model_validate(merged)
                except ValidationError as exc:
                    raise Refusal(
                        RefusalReason.INVALID_CONFIG,
                        f"the change to {site_id!r} is not a valid site",
                        site=site_id,
                        errors=exc.errors(include_url=False, include_context=False),
                    ) from exc
                entries[index] = updated
                self._write(entries)
                return updated
        raise Refusal(RefusalReason.SITE_NOT_FOUND, f"no site is named {site_id!r}", site=site_id)

    def remove(self, site_id: str) -> SiteEntry:
        """Remove a site and return what it was."""
        with self._locked():
            entries = self._read()
            for index, entry in enumerate(entries):
                if entry.id == site_id:
                    del entries[index]
                    self._write(entries)
                    return entry
        raise Refusal(RefusalReason.SITE_NOT_FOUND, f"no site is named {site_id!r}", site=site_id)

    def switch(self, site_id: str, adapter: str) -> SiteEntry:
        """Make an alternative the adapter in use. The old one becomes an alternative."""
        entry = self.get(site_id)
        if adapter == entry.adapter:
            return entry
        if adapter not in entry.alternatives:
            raise Refusal(
                RefusalReason.INVALID_ARGUMENTS,
                f"{site_id!r} has no alternative named {adapter!r}",
                site=site_id,
                alternatives=sorted(entry.alternatives),
            )
        alternatives = {name: config for name, config in entry.alternatives.items()}
        config = alternatives.pop(adapter)
        alternatives = {entry.adapter: entry.config} | alternatives
        return self.edit(
            site_id, {"adapter": adapter, "config": config, "alternatives": alternatives}
        )
