"""A small builder for A2UI components, limited to relay's catalog (RL33).

Relay's catalog is a subset of the Basic catalog plus CLIO's choice table
(:data:`TABLE`). :data:`COMPONENTS` names them, and :class:`Tree` refuses any
other component, so a surface can never ask CLIO for a component it does not
already render. Only a client that lists relay's catalog gets the table.
"""

from __future__ import annotations

import re
from typing import Any

COMPONENTS = frozenset(
    {
        "Text",
        "Row",
        "Column",
        "Card",
        "Tabs",
        "Divider",
        "Button",
        "TextField",
        "CheckBox",
        "ChoicePicker",
        "clio.choice-table.v1",
    }
)
TABLE = "clio.choice-table.v1"

Component = dict[str, Any]


def slug(value: str) -> str:
    """Return ``value`` as a safe piece of a component id."""
    return re.sub(r"[^A-Za-z0-9_-]+", "-", value).strip("-") or "x"


class Tree:
    """Collects one surface's components. Ids are unique within the tree."""

    def __init__(self) -> None:
        self.components: list[Component] = []
        self._ids: set[str] = set()

    def add(self, component_id: str, component: str, **props: Any) -> str:
        """Add one component and return its id."""
        if component not in COMPONENTS:
            raise ValueError(f"{component} is not in relay's catalog")
        if component_id in self._ids:
            raise ValueError(f"component id {component_id!r} is used twice")
        self._ids.add(component_id)
        self.components.append(
            {"id": component_id, "component": component}
            | {key: value for key, value in props.items() if value is not None}
        )
        return component_id

    def text(self, component_id: str, text: str | dict[str, Any], variant: str = "body") -> str:
        return self.add(component_id, "Text", text=text, variant=variant)

    def column(self, component_id: str, children: list[str], **props: Any) -> str:
        return self.add(component_id, "Column", children=children, **props)

    def row(self, component_id: str, children: list[str], **props: Any) -> str:
        return self.add(component_id, "Row", children=children, **props)

    def card(self, component_id: str, child: str) -> str:
        return self.add(component_id, "Card", child=child)

    def divider(self, component_id: str) -> str:
        return self.add(component_id, "Divider")

    def button(
        self,
        component_id: str,
        label: str,
        action: dict[str, Any],
        variant: str = "default",
    ) -> str:
        """Add a button labelled ``label``."""
        label_id = self.text(f"{component_id}-label", label)
        return self.add(component_id, "Button", child=label_id, action=action, variant=variant)

    def fact(self, component_id: str, label: str, value: str) -> str:
        """Add a caption and its value side by side, with no separator between them."""
        caption = self.text(f"{component_id}-k", label, "caption")
        body = self.text(f"{component_id}-v", value)
        return self.row(component_id, [caption, body], justify="spaceBetween", align="start")


def event(name: str, context: dict[str, Any]) -> dict[str, Any]:
    """Return a button action that calls the tool ``name`` with ``context`` as its arguments."""
    return {"event": {"name": name, "context": context}}


def open_url(url: str) -> dict[str, Any]:
    """Return a button action that opens ``url``."""
    return {"functionCall": {"call": "openUrl", "args": {"url": url}}}


def bound(path: str) -> dict[str, str]:
    """Return a data binding to ``path`` in the surface's data model."""
    return {"path": path}
