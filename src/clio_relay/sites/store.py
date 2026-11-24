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
from pydantic import BaseModel, ConfigDict, ValidationError

from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import CatalogEntry, SiteEntry, SitePolicy, SpendAsk, SpendPolicy


class NewSiteDefaults(BaseModel):
    """What a new site starts with when whoever adds it leaves the choice out."""

    model_config = ConfigDict(extra="forbid")

    adapter: str = "ssh"
    policy: SitePolicy = "guarded"
    spend: SpendAsk = "always"
    """When relay asks before spending on a new site: ``spend.ask`` of :class:`SpendPolicy`."""


def _dump_atomic(path: Path, data: Any) -> None:
    text = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.stem}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


class SiteStore:
    """Reads and writes the person's ``sites.yaml``, a top-level list of entries.

    The defaults for new sites sit beside it, in ``site-defaults.yaml``.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.defaults_path = path.with_name("site-defaults.yaml")

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
        _dump_atomic(self.path, data)

    def defaults(self) -> NewSiteDefaults:
        """Return the adapter, policy and spend setting a new site starts with."""
        try:
            loaded: object = yaml.safe_load(self.defaults_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return NewSiteDefaults()
        try:
            return NewSiteDefaults.model_validate(loaded or {})
        except ValidationError as exc:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                f"{self.defaults_path} is not valid site defaults",
                path=str(self.defaults_path),
                errors=exc.errors(include_url=False, include_context=False),
            ) from exc

    def set_defaults(
        self,
        adapter: str | None = None,
        policy: str | None = None,
        spend: str | float | None = None,
    ) -> NewSiteDefaults:
        """Change the defaults for new sites; what is left out stays as it was.

        The caller checks that ``adapter`` is an installed adapter.
        """
        with self._locked():
            changes = {
                key: value
                for key, value in (("adapter", adapter), ("policy", policy), ("spend", spend))
                if value is not None
            }
            try:
                updated = NewSiteDefaults.model_validate(self.defaults().model_dump() | changes)
            except ValidationError as exc:
                raise Refusal(
                    RefusalReason.INVALID_ARGUMENTS,
                    "those are not valid defaults for new sites",
                    errors=exc.errors(include_url=False, include_context=False),
                ) from exc
            _dump_atomic(self.defaults_path, updated.model_dump())
        return updated

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

    def add_from_catalog(
        self,
        catalog: CatalogEntry,
        site_id: str | None = None,
        adapter: str | None = None,
        changes: dict[str, Any] | None = None,
    ) -> SiteEntry:
        """Copy a catalog entry into ``sites.yaml``, where the person edits it.

        What is added is :meth:`from_catalog`'s site.
        """
        return self.add(self.from_catalog(catalog, site_id, adapter, changes))

    def from_catalog(
        self,
        catalog: CatalogEntry,
        site_id: str | None = None,
        adapter: str | None = None,
        changes: dict[str, Any] | None = None,
    ) -> SiteEntry:
        """Return the site a catalog entry becomes, without adding it.

        ``adapter`` picks one of the entry's alternatives as the protocol in use,
        and ``changes`` replaces top-level fields (a name, a config) first.
        An entry that sets no security or spend setting takes the person's default
        for new sites (Secure, and always ask, unless they changed them).
        """
        site = catalog.to_site(site_id, adapter)
        given = changes or {}
        if given:
            site = SiteEntry.model_validate(site.model_dump(mode="json") | given)
        defaults = self.defaults()
        update: dict[str, Any] = {}
        if "policy" not in catalog.model_fields_set and "policy" not in given:
            update["policy"] = defaults.policy
        if "spend" not in catalog.model_fields_set and "spend" not in given:
            update["spend"] = SpendPolicy(ask=defaults.spend)
        return site.model_copy(update=update) if update else site

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
