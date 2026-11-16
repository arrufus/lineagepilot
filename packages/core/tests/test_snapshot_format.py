"""A change to what a snapshot holds must change SNAPSHOT_FORMAT, so a rollback stays safe.

The language server caches the graph as a snapshot, keyed by the core's version and
``SNAPSHOT_FORMAT``, and drops one whose key does not match. The core's version does not change
between releases, so the format is the only guard. If the graph model changed and the format did
not, a server of an earlier release, after a rollback, would load a snapshot written by a later
one, which it cannot read correctly (docs/release.md, "Rolling back to an earlier release").

When this fails: bump SNAPSHOT_FORMAT in snapshot.py, and record the new model's fingerprint
under the new number below.
"""

from __future__ import annotations

import dataclasses
import enum
import hashlib
import inspect

from lineagepilot_core.graph import model
from lineagepilot_core.snapshot import SNAPSHOT_FORMAT

# SNAPSHOT_FORMAT -> the fingerprint of the graph model it holds
MODEL_OF_FORMAT = {
    2: "97e717244a3dcfe9a82a679ad3b7420900be58e9a142ad3d1f2ab4cfacb797b0",
    # The same model, written as JSON lines in batches (LP-dtt)
    3: "97e717244a3dcfe9a82a679ad3b7420900be58e9a142ad3d1f2ab4cfacb797b0",
}


def model_shape() -> str:
    """Every enum's values and every record's fields in the graph model, as text."""
    parts = []
    for name, obj in sorted(vars(model).items()):
        if not (inspect.isclass(obj) and obj.__module__ == model.__name__):
            continue
        if issubclass(obj, enum.Enum):
            parts.append(f"{name}: " + ",".join(m.value for m in obj))
        elif dataclasses.is_dataclass(obj):
            parts.append(f"{name}(" + ",".join(f.name for f in dataclasses.fields(obj)) + ")")
    return "\n".join(parts)


def test_the_snapshot_format_names_the_model_it_holds():
    fingerprint = hashlib.sha256(model_shape().encode()).hexdigest()
    assert MODEL_OF_FORMAT.get(SNAPSHOT_FORMAT) == fingerprint, (
        "The graph model changed, so snapshots written now differ from older ones. Bump "
        f"SNAPSHOT_FORMAT (now {SNAPSHOT_FORMAT}) and add {{<new format>: {fingerprint!r}}} to "
        "MODEL_OF_FORMAT, so a server of an earlier release ignores the new snapshots after a "
        "rollback instead of loading them."
    )


def test_the_shape_covers_the_records_and_kinds_a_snapshot_holds():
    shape = model_shape()
    for record in ("Node(", "Column(", "Edge(", "Unresolved(", "Evidence(", "Location("):
        assert record in shape
    assert "NodeKind: model," in shape and "EdgeKind: depends_on," in shape
