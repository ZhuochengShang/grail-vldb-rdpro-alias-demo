"""Deterministic semantic comparison for Python-oracle and Scala outputs."""

from __future__ import annotations

import csv
import math
from pathlib import Path


PASS = "GROUND_TRUTH_PASS"
WRONG = "RUNNABLE_BUT_WRONG"
UNAVAILABLE = "COMPARISON_UNAVAILABLE"


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if path.is_dir():
        parts = sorted(p for p in path.glob("part-*") if p.is_file())
        lines = []
        for part in parts:
            lines.extend(line for line in part.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip())
        if not lines:
            raise ValueError("Spark output directory has no non-empty part files")
        reader = csv.DictReader(lines)
    else:
        stream = path.open("r", encoding="utf-8", errors="ignore", newline="")
        try:
            reader = csv.DictReader(stream)
            fields = list(reader.fieldnames or [])
            rows = [dict(row) for row in reader]
        finally:
            stream.close()
        return fields, rows
    return list(reader.fieldnames or []), [dict(row) for row in reader]


def _number(value: str):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def compare_csv(left: Path, right: Path, atol: float, rtol: float) -> dict:
    left_fields, left_rows = _read_csv(left)
    right_fields, right_rows = _read_csv(right)
    if set(left_fields) != set(right_fields):
        return {"status": WRONG, "kind": "csv", "reason": "schema mismatch",
                "expected_columns": left_fields, "actual_columns": right_fields}
    fields = sorted(left_fields)
    if len(left_rows) != len(right_rows):
        return {"status": WRONG, "kind": "csv", "reason": "row-count mismatch",
                "expected_rows": len(left_rows), "actual_rows": len(right_rows)}

    def canonical(row):
        return tuple(str(row.get(field, "")).strip() for field in fields)

    left_rows = sorted(left_rows, key=canonical)
    right_rows = sorted(right_rows, key=canonical)
    mismatches = []
    max_abs_error = 0.0
    for index, (expected, actual) in enumerate(zip(left_rows, right_rows)):
        for field in fields:
            exp_text = str(expected.get(field, "")).strip()
            act_text = str(actual.get(field, "")).strip()
            exp_num, act_num = _number(exp_text), _number(act_text)
            if exp_num is not None and act_num is not None:
                error = abs(exp_num - act_num)
                max_abs_error = max(max_abs_error, error)
                equal = error <= atol + rtol * abs(exp_num)
            else:
                equal = exp_text == act_text
            if not equal:
                mismatches.append({"row": index, "column": field,
                                   "expected": exp_text, "actual": act_text})
                if len(mismatches) >= 20:
                    break
        if len(mismatches) >= 20:
            break
    return {
        "status": PASS if not mismatches else WRONG,
        "kind": "csv",
        "reason": "all rows match" if not mismatches else "value mismatch",
        "rows": len(left_rows),
        "columns": fields,
        "max_abs_error": max_abs_error,
        "mismatches": mismatches,
    }


def compare_geotiff(left: Path, right: Path, atol: float, rtol: float) -> dict:
    try:
        import numpy as np
        import rasterio
    except ImportError as exc:
        return {"status": UNAVAILABLE, "kind": "geotiff", "reason": str(exc)}
    with rasterio.open(left) as expected, rasterio.open(right) as actual:
        metadata = {
            "crs": (str(expected.crs), str(actual.crs)),
            "shape": ((expected.count, expected.height, expected.width),
                      (actual.count, actual.height, actual.width)),
            "transform": (tuple(expected.transform), tuple(actual.transform)),
        }
        if metadata["crs"][0] != metadata["crs"][1]:
            return {"status": WRONG, "kind": "geotiff", "reason": "CRS mismatch", "metadata": metadata}
        if metadata["shape"][0] != metadata["shape"][1]:
            return {"status": WRONG, "kind": "geotiff", "reason": "shape mismatch", "metadata": metadata}
        if not np.allclose(metadata["transform"][0], metadata["transform"][1], atol=atol, rtol=rtol):
            return {"status": WRONG, "kind": "geotiff", "reason": "transform mismatch", "metadata": metadata}
        exp = expected.read(masked=True).astype("float64")
        act = actual.read(masked=True).astype("float64")
        exp_mask, act_mask = np.ma.getmaskarray(exp), np.ma.getmaskarray(act)
        if not np.array_equal(exp_mask, act_mask):
            return {"status": WRONG, "kind": "geotiff", "reason": "NoData mask mismatch", "metadata": metadata}
        valid = ~exp_mask
        differences = np.abs(exp.data[valid] - act.data[valid])
        max_abs_error = float(differences.max()) if differences.size else 0.0
        equal = np.allclose(exp.data[valid], act.data[valid], atol=atol, rtol=rtol)
        return {"status": PASS if equal else WRONG, "kind": "geotiff",
                "reason": "all pixels match" if equal else "pixel mismatch",
                "valid_pixels": int(valid.sum()), "max_abs_error": max_abs_error,
                "metadata": metadata}


def compare_output_paths(expected: Path, actual: Path, atol: float = 1e-6,
                         rtol: float = 1e-6) -> dict:
    if not expected.exists() or not actual.exists():
        return {"status": UNAVAILABLE, "reason": "one or both output paths do not exist",
                "expected": str(expected), "actual": str(actual)}
    expected_suffix = expected.suffix.lower()
    actual_suffix = actual.suffix.lower()
    try:
        if expected_suffix == ".csv" and (actual_suffix == ".csv" or actual.is_dir()):
            result = compare_csv(expected, actual, atol, rtol)
        elif expected_suffix in {".tif", ".tiff"} and actual_suffix in {".tif", ".tiff"}:
            result = compare_geotiff(expected, actual, atol, rtol)
        else:
            result = {"status": UNAVAILABLE, "reason": "unsupported or unlike output formats",
                      "expected_format": expected_suffix, "actual_format": actual_suffix}
    except Exception as exc:
        result = {"status": UNAVAILABLE, "reason": f"comparison failed: {type(exc).__name__}: {exc}"}
    result.update({"expected": str(expected), "actual": str(actual), "atol": atol, "rtol": rtol})
    return result
