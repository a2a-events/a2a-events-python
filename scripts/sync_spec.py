"""Sync the vendored spec contract from the source of truth.

The language-neutral contract — the published JSON Schemas and the conformance
fixtures — is owned by the spec repo (the source of truth):

    https://github.com/a2a-events/a2a-events

This repo vendors a copy under ``schemas/`` and ``conformance/fixtures/`` so it
stays self-contained (tests and CI need no second checkout). This script
refreshes those copies from the spec repo.

Run: ``uv run python scripts/sync_spec.py``

By default it copies from a sibling checkout at ``../a2a-events`` when present,
otherwise it fetches the files over HTTPS from the spec repo on ``main``.

    uv run python scripts/sync_spec.py                       # auto: sibling or GitHub
    uv run python scripts/sync_spec.py --spec-dir ../a2a-events   # force a local checkout
    uv run python scripts/sync_spec.py --ref v0.1.0          # pin a tag/branch when fetching
    uv run python scripts/sync_spec.py --check               # fail if anything is out of sync

After syncing, ``tests/test_conformance.py`` validates the schemas against the
models and the fixtures against this implementation.
"""

from __future__ import annotations

import argparse
import sys
import urllib.request
from pathlib import Path

# Source of truth: https://github.com/a2a-events/a2a-events
SPEC_REPO = "a2a-events/a2a-events"
RAW_BASE = "https://raw.githubusercontent.com/" + SPEC_REPO

REPO = Path(__file__).resolve().parent.parent  # a2a-events-python/

# Vendored path (relative to each repo root) -> mirrored on both sides.
VENDORED = [
    "schemas/error.schema.json",
    "schemas/event.schema.json",
    "schemas/selector.schema.json",
    "schemas/subscription.schema.json",
    "schemas/topic.schema.json",
    "conformance/fixtures/selectors.json",
    "conformance/fixtures/cursors.json",
    "conformance/fixtures/errors.json",
]


def _read_source(rel: str, spec_dir: Path | None, ref: str) -> bytes:
    if spec_dir is not None:
        return (spec_dir / rel).read_bytes()
    url = f"{RAW_BASE}/{ref}/{rel}"
    with urllib.request.urlopen(url) as resp:  # noqa: S310 - fixed trusted host
        return resp.read()


def _resolve_spec_dir(arg: str | None) -> Path | None:
    if arg:
        d = Path(arg).resolve()
        if not d.is_dir():
            sys.exit(f"--spec-dir {d} does not exist")
        return d
    sibling = (REPO.parent / "a2a-events").resolve()
    return sibling if (sibling / "schemas").is_dir() else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--spec-dir",
        help="local checkout of the spec repo to copy from (default: ../a2a-events if present)",
    )
    parser.add_argument(
        "--ref",
        default="main",
        help="git ref to fetch over HTTPS when no local checkout is used (default: main)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="do not write; exit non-zero if any vendored file is out of sync",
    )
    args = parser.parse_args()

    spec_dir = _resolve_spec_dir(args.spec_dir)
    origin = str(spec_dir) if spec_dir else f"{RAW_BASE}/{args.ref}"
    print(f"source of truth: {origin}")

    drift = []
    for rel in VENDORED:
        source = _read_source(rel, spec_dir, args.ref)
        dest = REPO / rel
        current = dest.read_bytes() if dest.exists() else None
        if current == source:
            continue
        drift.append(rel)
        if args.check:
            print(f"  OUT OF SYNC  {rel}")
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(source)
        print(f"  synced  {rel}")

    if args.check and drift:
        print(f"\n{len(drift)} file(s) out of sync; run scripts/sync_spec.py")
        return 1
    if not drift:
        print("already in sync")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
