"""Jobs, operations, refusals and the content store: relay's core data model.

The names are loaded from their modules on first use, so importing one light
module, such as :mod:`clio_relay.jobs.refusals`, does not load the rest.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from clio_relay.jobs.content import ContentStore
    from clio_relay.jobs.handles import (
        STALL_S,
        Endpoint,
        EndpointPort,
        ExecResult,
        FileAccess,
        FileEntry,
        JobHandle,
        JobState,
        JobStatus,
        OutputRef,
        StallReport,
    )
    from clio_relay.jobs.records import (
        OperationKind,
        OperationRecord,
        OperationState,
        RecordStore,
        new_id,
    )
    from clio_relay.jobs.refusals import Refusal, RefusalReason
    from clio_relay.jobs.spec import (
        InputRef,
        JobAttributes,
        JobResources,
        JobSpec,
        PackageRef,
        ServicePort,
        ServiceSpec,
        canonical_digest,
    )

_HOMES = {
    "ContentStore": "clio_relay.jobs.content",
    "STALL_S": "clio_relay.jobs.handles",
    "Endpoint": "clio_relay.jobs.handles",
    "EndpointPort": "clio_relay.jobs.handles",
    "ExecResult": "clio_relay.jobs.handles",
    "FileAccess": "clio_relay.jobs.handles",
    "FileEntry": "clio_relay.jobs.handles",
    "JobHandle": "clio_relay.jobs.handles",
    "JobState": "clio_relay.jobs.handles",
    "JobStatus": "clio_relay.jobs.handles",
    "OutputRef": "clio_relay.jobs.handles",
    "StallReport": "clio_relay.jobs.handles",
    "OperationKind": "clio_relay.jobs.records",
    "OperationRecord": "clio_relay.jobs.records",
    "OperationState": "clio_relay.jobs.records",
    "RecordStore": "clio_relay.jobs.records",
    "new_id": "clio_relay.jobs.records",
    "Refusal": "clio_relay.jobs.refusals",
    "RefusalReason": "clio_relay.jobs.refusals",
    "InputRef": "clio_relay.jobs.spec",
    "JobAttributes": "clio_relay.jobs.spec",
    "JobResources": "clio_relay.jobs.spec",
    "JobSpec": "clio_relay.jobs.spec",
    "PackageRef": "clio_relay.jobs.spec",
    "ServicePort": "clio_relay.jobs.spec",
    "ServiceSpec": "clio_relay.jobs.spec",
    "canonical_digest": "clio_relay.jobs.spec",
}

__all__ = [
    "STALL_S",
    "ContentStore",
    "Endpoint",
    "EndpointPort",
    "ExecResult",
    "FileAccess",
    "FileEntry",
    "InputRef",
    "JobAttributes",
    "JobHandle",
    "JobResources",
    "JobSpec",
    "JobState",
    "JobStatus",
    "OperationKind",
    "OperationRecord",
    "OperationState",
    "OutputRef",
    "PackageRef",
    "RecordStore",
    "Refusal",
    "RefusalReason",
    "ServicePort",
    "ServiceSpec",
    "StallReport",
    "canonical_digest",
    "new_id",
]


def __getattr__(name: str) -> Any:
    home = _HOMES.get(name)
    if home is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getattr(importlib.import_module(home), name)
