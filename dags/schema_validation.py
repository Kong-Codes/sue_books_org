import json
import os
from typing import Dict, List, Any

import polars as pl

from utils import get_logger, send_alert


log = get_logger(__name__, log_file="logs/schema_validation.log")


def _polars_dtype_to_str(dtype: pl.datatypes.DataType) -> str:
    # Normalize Polars dtype to a stable string
    return str(dtype)


def infer_schema(df: pl.DataFrame) -> List[Dict[str, Any]]:
    schema: List[Dict[str, Any]] = []
    null_counts = df.null_count().to_dict(as_series=False)
    for col, dtype in df.schema.items():
        nullable = bool(null_counts.get(col, [0])[0] > 0)
        schema.append({
            "name": col,
            "type": _polars_dtype_to_str(dtype),
            "nullable": nullable,
        })
    # sort for stable ordering
    schema.sort(key=lambda c: c["name"])
    return schema


def _index_by_name(cols: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {c["name"]: c for c in cols}


def compare_schemas(baseline: List[Dict[str, Any]], current: List[Dict[str, Any]]) -> Dict[str, Any]:
    base_map = _index_by_name(baseline)
    curr_map = _index_by_name(current)

    added = [name for name in curr_map.keys() if name not in base_map]
    removed = [name for name in base_map.keys() if name not in curr_map]

    type_changes: List[Dict[str, Any]] = []
    nullability_changes: List[Dict[str, Any]] = []
    for name in base_map.keys() & curr_map.keys():
        b = base_map[name]
        c = curr_map[name]
        if b["type"] != c["type"]:
            type_changes.append({"column": name, "from": b["type"], "to": c["type"]})
        if bool(b.get("nullable", False)) != bool(c.get("nullable", False)):
            nullability_changes.append({"column": name, "from": b.get("nullable", False), "to": c.get("nullable", False)})

    drift_detected = bool(added or removed or type_changes or nullability_changes)
    return {
        "drift_detected": drift_detected,
        "added_columns": added,
        "removed_columns": removed,
        "type_changes": type_changes,
        "nullability_changes": nullability_changes,
    }


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _write_json(path: str, data: Any) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def _read_json(path: str) -> Any:
    with open(path, "r") as f:
        return json.load(f)


def validate_and_report(datasets: Dict[str, pl.DataFrame], artifacts_dir: str = "/opt/airflow/datasets/processed/schema") -> Dict[str, Any]:
    """
    Validate current dataset schemas against baseline. If baseline for a dataset doesn't
    exist, create it. Always write a report JSON per dataset. Returns an overall summary.
    """
    _ensure_dir(artifacts_dir)
    baselines_dir = os.path.join(artifacts_dir, "baselines")
    reports_dir = os.path.join(artifacts_dir, "reports")
    _ensure_dir(baselines_dir)
    _ensure_dir(reports_dir)

    overall = {"datasets": {}, "any_drift": False}

    for name, df in datasets.items():
        current_schema = infer_schema(df)
        baseline_path = os.path.join(baselines_dir, f"{name}.schema.json")
        report_path = os.path.join(reports_dir, f"{name}.report.json")

        if not os.path.exists(baseline_path):
            _write_json(baseline_path, current_schema)
            result = {
                "status": "baseline_created",
                "drift": {"drift_detected": False, "added_columns": [], "removed_columns": [], "type_changes": [], "nullability_changes": []},
            }
            _write_json(report_path, result)
            overall["datasets"][name] = result
            log.info(f"Baseline schema created for {name} at {baseline_path}")
            continue

        baseline_schema = _read_json(baseline_path)
        drift = compare_schemas(baseline_schema, current_schema)
        status = "drift_detected" if drift["drift_detected"] else "no_drift"
        result = {"status": status, "drift": drift}
        _write_json(report_path, result)
        overall["datasets"][name] = result
        overall["any_drift"] = overall["any_drift"] or drift["drift_detected"]

    # If drift, send a single alert with highlights
    if overall["any_drift"]:
        highlights = {}
        for name, res in overall["datasets"].items():
            if res["drift"]["drift_detected"]:
                d = res["drift"]
                highlights[f"{name}_added"] = d["added_columns"]
                highlights[f"{name}_removed"] = d["removed_columns"]
                highlights[f"{name}_type_changes"] = d["type_changes"]
                highlights[f"{name}_nullability_changes"] = d["nullability_changes"]
        send_alert("Schema Drift Detected", highlights, level="warn")

    return overall



