import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ui"))
from output_compare import PASS, WRONG, compare_output_paths


def _csv(path: Path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["name", "value"])
        writer.writeheader()
        writer.writerows(rows)


def test_csv_comparison_is_order_independent_and_numeric(tmp_path):
    expected, actual = tmp_path / "expected.csv", tmp_path / "actual.csv"
    _csv(expected, [{"name": "a", "value": "1.0"}, {"name": "b", "value": "2"}])
    _csv(actual, [{"name": "b", "value": "2.0000001"}, {"name": "a", "value": "1"}])
    assert compare_output_paths(expected, actual)["status"] == PASS


def test_csv_comparison_rejects_wrong_values(tmp_path):
    expected, actual = tmp_path / "expected.csv", tmp_path / "actual.csv"
    _csv(expected, [{"name": "a", "value": "1"}])
    _csv(actual, [{"name": "a", "value": "9"}])
    result = compare_output_paths(expected, actual)
    assert result["status"] == WRONG
    assert result["mismatches"]
