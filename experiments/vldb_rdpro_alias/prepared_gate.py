"""Admission checks for prevalidated VLDB demo cases."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path


def validate_case(case_dir: Path) -> tuple[bool, list[str]]:
    errors: list[str] = []
    required = ["READY", "task.json", "api_plan.json", "prepared.scala",
                "comparison.json", "fixture_manifest.json"]
    for name in required:
        if not (case_dir / name).exists():
            errors.append(f"missing {name}")
    if errors:
        return False, errors
    try:
        comparison = json.loads((case_dir / "comparison.json").read_text())
        if comparison.get("status") != "GROUND_TRUTH_PASS":
            errors.append("comparison is not GROUND_TRUTH_PASS")
        manifest = json.loads((case_dir / "fixture_manifest.json").read_text())
        for rel, expected in manifest.items():
            path = Path(rel)
            if not path.exists():
                errors.append(f"fixture missing: {rel}")
            elif hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                errors.append(f"fixture hash mismatch: {rel}")
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"invalid prepared metadata: {exc}")
    return not errors, errors


def ready_cases(root: Path) -> dict[str, Path]:
    result: dict[str, Path] = {}
    if not root.exists():
        return result
    for case_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        ok, _ = validate_case(case_dir)
        if ok:
            result[case_dir.name] = case_dir
    return result
