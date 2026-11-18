"""Forms generated from an adapter's ``Config`` JSON Schema (RL32).

Each property becomes one field of a component CLIO already renders:

* a string is a short text field, a number a number field;
* a boolean is a check box, an enum a single choice;
* a list of strings is a long text field with one item per line;
* anything else (a mapping, a nested model) is a long text field of YAML.

Drafts live in the surface's data model as strings, booleans and lists, which
is what those components bind to. :func:`parse` turns a draft back into the
mapping the adapter's ``Config`` validates.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Literal, cast

import yaml

Kind = Literal["text", "number", "integer", "bool", "choice", "lines", "yaml"]

_WORDS = {
    "api": "API",
    "cpus": "CPUs",
    "gpus": "GPUs",
    "id": "ID",
    "ids": "IDs",
    "iri": "IRI",
    "oauth": "OAuth",
    "pki": "PKI",
    "scp": "scp",
    "ssh": "SSH",
    "tms": "TMS",
    "url": "URL",
    "urls": "URLs",
}


def humanize(key: str) -> str:
    """Return a field label for a property name.

    ``connect_timeout_s`` becomes ``Connect timeout (seconds)``.
    """
    seconds = key.endswith("_s")
    words = (key[:-2] if seconds else key).split("_")
    shown = [_WORDS.get(word, word) for word in words if word]
    if shown and shown[0] == shown[0].lower():
        shown[0] = shown[0][:1].upper() + shown[0][1:]
    label = " ".join(shown)
    return f"{label} (seconds)" if seconds else label


@dataclass(frozen=True)
class Field:
    """One generated form field."""

    key: str
    label: str
    kind: Kind
    help: str | None = None
    required: bool = False
    options: tuple[str, ...] = ()
    labels: tuple[str, ...] = ()
    default: Any = None

    def option_labels(self) -> list[dict[str, str]]:
        """Return the choice's options as ChoicePicker options."""
        shown = self.labels or self.options
        return [
            {"label": label, "value": value}
            for label, value in zip(shown, self.options, strict=True)
        ]


@dataclass(frozen=True)
class Form:
    """The fields of one adapter's ``Config``."""

    adapter: str
    fields: tuple[Field, ...] = field(default_factory=tuple[Field, ...])
    # Offered for a new site or a new way to reach one. A test-only adapter is not.
    listed: bool = True


def _resolve(schema: dict[str, Any], defs: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Return the property's own schema, following ``$ref``, and whether null is allowed."""
    nullable = False
    branches = schema.get("anyOf") or schema.get("oneOf")
    if isinstance(branches, list):
        kept = [
            cast(dict[str, Any], item)
            for item in cast(list[Any], branches)
            if isinstance(item, dict) and cast(dict[str, Any], item).get("type") != "null"
        ]
        nullable = len(kept) < len(cast(list[Any], branches))
        schema = {**schema, **kept[0]} if len(kept) == 1 else {**schema, "type": "object"}
    ref = schema.get("$ref")
    if isinstance(ref, str) and ref.startswith("#/$defs/"):
        schema = {**defs.get(ref.removeprefix("#/$defs/"), {}), **schema}
        schema.pop("$ref", None)
    return schema, nullable


def _kind(schema: dict[str, Any]) -> Kind:
    if isinstance(schema.get("enum"), list):
        return "choice"
    kind = schema.get("type")
    if kind == "boolean":
        return "bool"
    if kind == "integer":
        return "integer"
    if kind == "number":
        return "number"
    if kind == "string":
        return "text"
    if kind == "array":
        items = schema.get("items")
        if isinstance(items, dict) and cast(dict[str, Any], items).get("type") == "string":
            return "lines"
    return "yaml"


def form(adapter: str, schema: dict[str, Any], *, listed: bool = True) -> Form:
    """Return the form for an adapter's ``Config`` schema."""
    defs = cast(dict[str, Any], schema.get("$defs") or {})
    required = set(cast(list[str], schema.get("required") or []))
    properties = cast(dict[str, Any], schema.get("properties") or {})
    fields: list[Field] = []
    for key, raw in properties.items():
        own, _nullable = _resolve(cast(dict[str, Any], raw), defs)
        kind = _kind(own)
        options = tuple(str(item) for item in cast(list[Any], own.get("enum") or []))
        fields.append(
            Field(
                key=key,
                label=humanize(key),
                kind=kind,
                help=cast(str | None, own.get("description")),
                required=key in required,
                options=options,
                default=own.get("default"),
            )
        )
    return Form(adapter=adapter, fields=tuple(fields), listed=listed)


def draft_value(item: Field, value: Any) -> Any:
    """Return what the field's component binds to for ``value`` (or its default when None)."""
    current = item.default if value is None else value
    if item.kind == "bool":
        return bool(current)
    if item.kind == "choice":
        return [str(current)] if current is not None else []
    if current is None:
        return ""
    if item.kind == "lines" and isinstance(current, list):
        return "\n".join(str(line) for line in cast(list[Any], current))
    if item.kind == "yaml":
        return yaml.safe_dump(current, sort_keys=False).strip() if current else ""
    return str(current)


def draft(fields: tuple[Field, ...], values: dict[str, Any]) -> dict[str, Any]:
    """Return a data-model draft for ``fields``, from saved ``values`` and the defaults."""
    return {item.key: draft_value(item, values.get(item.key)) for item in fields}


class FormError(ValueError):
    """A draft value that is not what its field takes."""

    def __init__(self, key: str, message: str) -> None:
        super().__init__(message)
        self.key = key


def parse_value(item: Field, raw: Any) -> Any:
    """Return the value one draft value stands for, or None when it is empty."""
    if item.kind == "bool":
        return bool(raw)
    if item.kind == "choice":
        chosen = cast(list[Any], raw) if isinstance(raw, list) else [raw]
        picked = [str(value) for value in chosen if value not in (None, "")]
        return picked[0] if picked else None
    text = "" if raw is None else str(raw)
    if item.kind == "lines":
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return lines or None
    if not text.strip():
        return None
    if item.kind == "text":
        return text.strip()
    if item.kind in ("number", "integer"):
        try:
            number = float(text.strip())
        except ValueError:
            raise FormError(item.key, "Enter a number") from None
        if item.kind == "integer":
            if not number.is_integer():
                raise FormError(item.key, "Enter a whole number")
            return int(number)
        return int(number) if number.is_integer() else number
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError:
        raise FormError(item.key, "Enter YAML") from None


def parse(fields: tuple[Field, ...], values: dict[str, Any]) -> dict[str, Any]:
    """Return the config a draft describes. A value equal to its default is left out."""
    config: dict[str, Any] = {}
    for item in fields:
        if item.key not in values:
            continue
        value = parse_value(item, values[item.key])
        if value is None or value == item.default:
            continue
        config[item.key] = value
    return config


_ID = re.compile(r"[^a-z0-9_-]+")


def suggest_id(name: str) -> str:
    """Return a site id made from a display name."""
    return _ID.sub("-", name.lower()).strip("-")[:63]
