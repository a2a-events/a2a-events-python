"""Write published JSON Schemas into ``schemas/``.

Run: ``uv run python scripts/export_schemas.py``

Regenerate and commit after changing ``models.py``;
``tests/test_conformance.py`` fails if the committed files drift.

These models are the generator for the language-neutral schema contract owned by
the spec repo (the source of truth): https://github.com/a2a-events/a2a-events .
After regenerating here, propagate the change to the spec repo so the published
contract and this vendored copy stay in lock-step.
"""

from __future__ import annotations

import json
from pathlib import Path

from a2a_events.schema_export import build_schemas

OUT = Path(__file__).resolve().parent.parent / "schemas"


def main() -> None:
    OUT.mkdir(exist_ok=True)
    for name, schema in build_schemas().items():
        (OUT / name).write_text(json.dumps(schema, indent=2) + "\n")
        print(f"wrote schemas/{name}")


if __name__ == "__main__":
    main()
