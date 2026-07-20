from __future__ import annotations

import base64
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

try:
    import streamlit as st
    import streamlit.components.v1 as components
except Exception as e:  # pragma: no cover
    raise SystemExit(
        "Streamlit is not installed. Install with: "
        "pip install streamlit\n"
        f"Original import error: {e}"
    )

from rdpro_section_codegen import (
    analyze_python_script,
    build_plan,
    build_section_specs,
    llm_infer_task_type,
    map_apis_to_sections,
)
from rdpro_section_codegen.compile_runner import resolve_cached_package_jars

# Repo-relative paths: this file lives at grail-agent/ui/grail_ui.py
GRAIL_AGENT_ROOT = Path(__file__).resolve().parents[1]   # grail-agent/
PROJECT_ROOT = GRAIL_AGENT_ROOT
REPO_ROOT = GRAIL_AGENT_ROOT.parent
PREPARED_GATE_DIR = (REPO_ROOT / "experiments" / "vldb_rdpro_alias").resolve()
sys.path.insert(0, str(PREPARED_GATE_DIR))
from prepared_gate import ready_cases  # noqa: E402
NOTEBOOK_DIR = GRAIL_AGENT_ROOT
CODEGEN_DIR = (GRAIL_AGENT_ROOT / "src" / "rdpro_section_codegen").resolve()
# Local machine environments (see configs/local_data_paths.md)
AGENT_PYTHON_ENV = Path("/Users/clockorangezoe/miniconda3/envs/geo_llm_spark/bin/python").resolve()
REFERENCE_PYTHON_ENV = Path("/Users/clockorangezoe/miniconda3/envs/GeoBenchX-main/bin/python").resolve()
SPARK_SUBMIT = Path("/Users/clockorangezoe/miniconda3/envs/geo_llm_spark/bin/spark-submit").resolve()
ENV_BIN = Path("/Users/clockorangezoe/miniconda3/envs/geo_llm_spark/bin").resolve()
RDPRO_LIB_DIR = Path(
    "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/LangGraph/"
    "beast-0.10.2-SNAPSHOT-bin/beast-0.10.2-SNAPSHOT/lib"
).resolve()

AGENT_SCRIPT = (CODEGEN_DIR / "langgraph_section_agent.py").resolve()
GUIDE_PATH = (GRAIL_AGENT_ROOT / "docs" / "RDProAgentLoop_perAPI_fix_migration_guide.md").resolve()
API_DOC_PATH = (GRAIL_AGENT_ROOT / "docs" / "rdpro_api_doc_combined.md").resolve()
BASE_SCAFFOLD = (GRAIL_AGENT_ROOT / "configs" / "job_scaffold.scala").resolve()
CANONICAL_GENERATED_SCALA = (GRAIL_AGENT_ROOT / "outputs" / "one_shot_output_sectional.scala").resolve()
SPARK_PACKAGES = (
    "org.locationtech.jts:jts-core:1.19.0,"
    "org.geotools:gt-referencing:24.1,"
    "org.geotools:gt-epsg-hsql:24.1,"
    "org.geotools:gt-geotiff:24.1,"
    "org.geotools:gt-coverage:24.1,"
    "it.geosolutions.imageio-ext:imageio-ext-tiff:1.4.14"
)
SPARK_REPOSITORIES = "https://repo.osgeo.org/repository/geotools-releases/,https://repo.osgeo.org/repository/release/"

UI_WORK = (NOTEBOOK_DIR / "ui_mock_runs").resolve()
UI_WORK.mkdir(parents=True, exist_ok=True)
SPARK_EVENTLOG_DIR = (UI_WORK / "spark_eventlog").resolve()
SPARK_EVENTLOG_DIR.mkdir(parents=True, exist_ok=True)

PYTHON_BOSTON_DETAIL_CSV = (PROJECT_ROOT / "python" / "boston_land_use_by_neighborhood_sample.csv").resolve()
PYTHON_BOSTON_SUMMARY_CSV = (PROJECT_ROOT / "python" / "boston_land_use_summary_sample.csv").resolve()
SCALA_BOSTON_DETAIL_CSV = (CODEGEN_DIR / "generated_output" / "boston_land_use_by_neighborhood_sample.csv").resolve()
SCALA_BOSTON_SUMMARY_CSV = (CODEGEN_DIR / "generated_output" / "boston_land_use_summary_sample.csv").resolve()

NLCD_CLASS_NAMES = {
    11: "Open Water",
    12: "Perennial Ice/Snow",
    21: "Developed, Open Space",
    22: "Developed, Low Intensity",
    23: "Developed, Medium Intensity",
    24: "Developed, High Intensity",
    31: "Barren Land",
    41: "Deciduous Forest",
    42: "Evergreen Forest",
    43: "Mixed Forest",
    52: "Shrub/Scrub",
    71: "Grassland/Herbaceous",
    81: "Pasture/Hay",
    82: "Cultivated Crops",
    90: "Woody Wetlands",
    95: "Emergent Herbaceous Wetlands",
}
DEVELOPED_CLASSES = {21, 22, 23, 24}
IMPERVIOUS_CLASSES = {22, 23, 24}
HIGH_INTENSITY_CLASSES = {23, 24}
FOREST_CLASSES = {41, 42, 43}
WETLAND_CLASSES = {90, 95}
AGRICULTURAL_CLASSES = {81, 82}
WATER_CLASSES = {11, 12}
SHRUB_GRASS_CLASSES = {52, 71}
BARREN_CLASSES = {31}


@dataclass
class CaseConfig:
    name: str
    vector_path: str
    raster_a_path: str
    raster_b_path: str
    output_path: str
    default_python: str


PRELOAD_CASES = {
    "NDVI_Landsat": CaseConfig(
        name="NDVI_Landsat",
        vector_path="",
        raster_a_path="file:///Users/clockorangezoe/Documents/phd_projects/code/geoAI/LangGraph/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF",
        raster_b_path="file:///Users/clockorangezoe/Documents/phd_projects/code/geoAI/LangGraph/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF",
        output_path="file:///Users/clockorangezoe/Documents/phd_projects/code/geoAI/LangGraph/run/api_test_jobs/ndvi_py2scala/apitest.tif",
        default_python="/Users/clockorangezoe/Documents/phd_projects/code/geoAI/LangGraph/python/ndvi.py",
    ),
    "CA_CDL_Landuse": CaseConfig(
        name="CA_CDL_Landuse",
        vector_path="file:///path/to/california.shp",
        raster_a_path="file:///path/to/cdl_landuse.tif",
        raster_b_path="",
        output_path="file:///path/to/cdl_california_crop.tif",
        default_python="/Users/clockorangezoe/Documents/phd_projects/code/geoAI/LangGraph/python/ndvi.py",
    ),
    "Boston_Zonal_Summary": CaseConfig(
        name="Boston_Zonal_Summary",
        vector_path="file:///Users/clockorangezoe/Downloads/boston_neighborhood_boundaries/Boston_Neighborhood_Boundaries_sample.shp",
        raster_a_path="file:///Users/clockorangezoe/Downloads/Annual_NLCD_LndCov_2024_CU_C1V1/Annual_NLCD_LndCov_2024_CU_C1V1.tif",
        raster_b_path="",
        output_path="file:///Users/clockorangezoe/Downloads/boston_land_use_summary.csv",
        default_python="/Users/clockorangezoe/Documents/phd_projects/code/geoAI/LangGraph/python/nlcd_clip_zonal.py",
    ),
    "Boston_Zonal_Detail": CaseConfig(
        name="Boston_Zonal_Detail",
        vector_path="file:///Users/clockorangezoe/Downloads/boston_neighborhood_boundaries/Boston_Neighborhood_Boundaries_sample.shp",
        raster_a_path="file:///Users/clockorangezoe/Downloads/Annual_NLCD_LndCov_2024_CU_C1V1/Annual_NLCD_LndCov_2024_CU_C1V1.tif",
        raster_b_path="",
        output_path="file:///Users/clockorangezoe/Downloads/boston_land_use_by_neighborhood.csv",
        default_python="/Users/clockorangezoe/Documents/phd_projects/code/geoAI/LangGraph/python/nlcd_clip_zonal.py",
    ),
}


def run_cmd(cmd: list[str], cwd: Path | None = None, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.run(cmd, text=True, capture_output=True, cwd=str(cwd) if cwd else None, env=merged_env)


def clear_work_dir(path: Path) -> None:
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            child.unlink(missing_ok=True)


def clear_ui_run_state() -> None:
    for key in [
        "python_output_path",
        "scala_output_path",
        "compare_python_override_path",
        "compare_scala_override_path",
        "pipeline_preview",
        "translate_state",
        "generated_scala_path",
        "agent_rc",
        "agent_out",
        "agent_err",
        "py_rc",
        "py_out",
        "py_err",
        "scala_rc",
        "scala_out",
        "scala_err",
        "status_msg",
    ]:
        st.session_state.pop(key, None)


def make_scaffold(base_scaffold: Path, vector_path: str, raster_a: str, raster_b: str, output_path: str) -> Path:
    txt = base_scaffold.read_text(encoding="utf-8", errors="ignore")
    # The canonical agent now injects SECTION_INPUTS from the Python script analysis.
    # Keep the UI scaffold as a clean copy of the canonical template.

    out = UI_WORK / "active_scaffold.scala"
    out.write_text(txt, encoding="utf-8")
    return out


def run_agent(scaffold_path: Path, python_input_path: Path) -> tuple[int, str, str, Path]:
    out_scala = CANONICAL_GENERATED_SCALA
    work_dir = UI_WORK / "agent_runs"
    work_dir.mkdir(parents=True, exist_ok=True)
    clear_work_dir(work_dir)
    out_scala.unlink(missing_ok=True)

    cmd = [
        str(AGENT_PYTHON_ENV),
        str(AGENT_SCRIPT),
        "--guide",
        str(GUIDE_PATH),
        "--api-doc",
        str(API_DOC_PATH),
        "--scaffold",
        str(scaffold_path),
        "--python-input",
        str(python_input_path),
        "--output-scala",
        str(out_scala),
        "--work-dir",
        str(work_dir),
        "--spark-submit",
        str(SPARK_SUBMIT),
        "--env-bin",
        str(ENV_BIN),
        "--rdpro-lib-dir",
        str(RDPRO_LIB_DIR),
    ]
    res = run_cmd(cmd, cwd=CODEGEN_DIR)
    return res.returncode, res.stdout, res.stderr, out_scala


def load_pipeline_view(py_path: Path, scaffold_path: Path) -> dict:
    if not py_path.exists():
        return {}

    py_text = py_path.read_text(encoding="utf-8", errors="ignore")
    analysis = analyze_python_script(py_text)
    inferred = llm_infer_task_type(py_text)
    analysis.task_type = inferred.get("task_type", analysis.task_type)
    analysis.task_label = inferred.get("task_label", analysis.task_label)
    plan = build_plan(analysis)
    scaffold_text = scaffold_path.read_text(encoding="utf-8", errors="ignore") if scaffold_path.exists() else ""
    section_specs = build_section_specs(analysis, plan, scaffold_text)
    section_api_map = map_apis_to_sections(analysis, plan.apis)
    return {
        "analysis": analysis.to_dict(),
        "task_info": inferred,
        "plan": plan.to_dict(),
        "section_api_map": section_api_map,
        "section_specs": section_specs,
    }


def latest_summary(work_dir: Path) -> tuple[Path | None, dict]:
    summaries = sorted(work_dir.glob("summary_*.json"))
    if not summaries:
        return None, {}
    latest = summaries[-1]
    try:
        summary = json.loads(latest.read_text(encoding="utf-8", errors="ignore"))
        if summary.get("output_scala") and Path(summary["output_scala"]).resolve() != CANONICAL_GENERATED_SCALA:
            return None, {}
        return latest, summary
    except Exception:
        return latest, {}


def run_python_script(py_path: Path) -> tuple[int, str, str]:
    ref_env_root = REFERENCE_PYTHON_ENV.parent.parent
    share_dir = ref_env_root / "share"
    python_env = {
        "PROJ_LIB": str(share_dir / "proj"),
        "PROJ_DATA": str(share_dir / "proj"),
        "GDAL_DATA": str(share_dir / "gdal"),
    }
    res = run_cmd([str(REFERENCE_PYTHON_ENV), str(py_path)], cwd=py_path.parent, env=python_env)
    return res.returncode, res.stdout, res.stderr


def run_scala_generated(scala_file: Path) -> tuple[int, str, str]:
    run_dir = UI_WORK / "scala_exec"
    run_dir.mkdir(parents=True, exist_ok=True)
    clear_work_dir(SPARK_EVENTLOG_DIR)

    scalac_path = shutil.which("scalac")
    jar_path = shutil.which("jar")
    if not scalac_path or not jar_path:
        return 2, "", "Missing scalac or jar on PATH"

    rdpro_jars = sorted(str(p) for p in RDPRO_LIB_DIR.glob("*.jar"))
    pyspark_jars_dir = (ENV_BIN / ".." / "lib" / "python3.10" / "site-packages" / "pyspark" / "jars").resolve()
    spark_jars = sorted(str(p) for p in pyspark_jars_dir.glob("*.jar")) if pyspark_jars_dir.exists() else []
    package_jars = resolve_cached_package_jars(SPARK_PACKAGES)
    if not rdpro_jars or not spark_jars:
        return 3, "", "Missing RDPro or PySpark jars"

    classes_dir = run_dir / "classes"
    classes_dir.mkdir(parents=True, exist_ok=True)
    app_jar = run_dir / "ui_generated.jar"

    compile_cp = ":".join(rdpro_jars + spark_jars + package_jars)
    c1 = run_cmd([
        scalac_path,
        "-classpath",
        compile_cp,
        "-d",
        str(classes_dir),
        str(scala_file),
    ], cwd=run_dir)
    if c1.returncode != 0:
        return c1.returncode, c1.stdout, c1.stderr

    c2 = run_cmd([
        jar_path,
        "cf",
        str(app_jar),
        "-C",
        str(classes_dir),
        ".",
    ], cwd=run_dir)
    if c2.returncode != 0:
        return c2.returncode, c2.stdout, c2.stderr

    c3 = run_cmd([
        str(SPARK_SUBMIT),
        "--class",
        "GeoJobMain",
        "--conf",
        "spark.eventLog.enabled=true",
        "--conf",
        f"spark.eventLog.dir={SPARK_EVENTLOG_DIR.as_uri()}",
        "--conf",
        "spark.eventLog.compress=false",
        "--repositories",
        SPARK_REPOSITORIES,
        "--packages",
        SPARK_PACKAGES,
        "--jars",
        ",".join(rdpro_jars),
        str(app_jar),
    ], cwd=run_dir)
    return c3.returncode, c3.stdout, c3.stderr


def extract_spark_dag_lines(text: str) -> str:
    lines = []
    for line in (text or "").splitlines():
        if any(token in line for token in ("Spark UI", "DAGScheduler", "Starting job:", "Final stage:", "Job ", "Stage ")):
            lines.append(line)
    return "\n".join(lines[-80:])


def fetch_json(url: str) -> list | dict | None:
    try:
        with urlopen(url, timeout=1.5) as resp:
            return json.loads(resp.read().decode("utf-8", errors="ignore"))
    except (URLError, TimeoutError, json.JSONDecodeError, ValueError):
        return None


def load_spark_ui_snapshot() -> dict:
    for port in (4040, 4041, 4042, 4043):
        base = f"http://localhost:{port}"
        apps = fetch_json(f"{base}/api/v1/applications")
        if not apps or not isinstance(apps, list):
            continue
        app = apps[0]
        app_id = app.get("id")
        if not app_id:
            continue
        jobs = fetch_json(f"{base}/api/v1/applications/{app_id}/jobs") or []
        stages = fetch_json(f"{base}/api/v1/applications/{app_id}/stages") or []
        executors = fetch_json(f"{base}/api/v1/applications/{app_id}/executors") or []
        return {
            "base": base,
            "app": app,
            "jobs": jobs if isinstance(jobs, list) else [],
            "stages": stages if isinstance(stages, list) else [],
            "executors": executors if isinstance(executors, list) else [],
        }
    return {}


def load_eventlog_snapshot(eventlog_dir: Path) -> dict:
    files = sorted([p for p in eventlog_dir.glob("*") if p.is_file()], key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return {}
    latest = files[0]
    app: dict = {}
    jobs: dict[int, dict] = {}
    stages: dict[int, dict] = {}
    try:
        with latest.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except Exception:
                    continue
                event_type = event.get("Event", "")
                if event_type == "SparkListenerApplicationStart":
                    app = {
                        "name": event.get("App Name", ""),
                        "id": event.get("App ID", ""),
                        "time": event.get("Timestamp", ""),
                    }
                elif event_type == "SparkListenerJobStart":
                    job_id = event.get("Job ID")
                    stage_ids = event.get("Stage IDs", []) or []
                    properties = event.get("Properties", {}) or {}
                    jobs[job_id] = {
                        "jobId": job_id,
                        "status": "RUNNING",
                        "stageIds": stage_ids,
                        "name": properties.get("spark.job.description", f"Job {job_id}"),
                        "numTasks": 0,
                        "numCompletedTasks": 0,
                        "numSkippedTasks": 0,
                        "stageIds": stage_ids,
                    }
                elif event_type == "SparkListenerJobEnd":
                    job_id = event.get("Job ID")
                    if job_id in jobs:
                        result = event.get("Job Result", {}) or {}
                        jobs[job_id]["status"] = result.get("Result", "SUCCEEDED")
                elif event_type == "SparkListenerStageSubmitted":
                    info = event.get("Stage Info", {}) or {}
                    stage_id = info.get("Stage ID")
                    stages[stage_id] = {
                        "stageId": stage_id,
                        "name": info.get("Stage Name", ""),
                        "status": "ACTIVE",
                        "numTasks": info.get("Number of Tasks", 0),
                        "numCompleteTasks": 0,
                        "inputBytes": 0,
                        "shuffleReadBytes": 0,
                        "shuffleWriteBytes": 0,
                        "parentIds": info.get("Parent IDs", []) or [],
                    }
                elif event_type == "SparkListenerStageCompleted":
                    info = event.get("Stage Info", {}) or {}
                    stage_id = info.get("Stage ID")
                    entry = stages.setdefault(stage_id, {})
                    entry.update(
                        {
                            "stageId": stage_id,
                            "name": info.get("Stage Name", entry.get("name", "")),
                            "status": "COMPLETE",
                            "numTasks": info.get("Number of Tasks", entry.get("numTasks", 0)),
                            "numCompleteTasks": info.get("Number of Completed Tasks", info.get("Number of Tasks", 0)),
                            "inputBytes": info.get("Task Metrics", {}).get("Input Metrics", {}).get("Bytes Read", 0) if isinstance(info.get("Task Metrics"), dict) else 0,
                            "shuffleReadBytes": info.get("Task Metrics", {}).get("Shuffle Read Metrics", {}).get("Remote Bytes Read", 0) if isinstance(info.get("Task Metrics"), dict) else 0,
                            "shuffleWriteBytes": info.get("Task Metrics", {}).get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0) if isinstance(info.get("Task Metrics"), dict) else 0,
                            "parentIds": info.get("Parent IDs", entry.get("parentIds", [])) or [],
                        }
                    )
    except Exception:
        return {}

    return {
        "base": f"eventlog:{latest.name}",
        "app": app,
        "jobs": list(jobs.values()),
        "stages": list(stages.values()),
        "executors": [],
    }


def build_stage_dag_dot_for_job(snapshot: dict, job: dict) -> str:
    stages = snapshot.get("stages", []) or []
    if not job or not stages:
        return ""

    stage_ids = set(job.get("stageIds", []) or [])
    stage_map = {stage.get("stageId"): stage for stage in stages if stage.get("stageId") in stage_ids or not stage_ids}
    if not stage_map:
        return ""

    lines = [
        "digraph SparkDAG {",
        'rankdir=LR;',
        'graph [pad="0.2", nodesep="0.35", ranksep="0.5"];',
        'node [shape=box, style="rounded,filled", color="#5b6b5f", fillcolor="#eef3e2", fontname="Helvetica", fontsize=10];',
        'edge [color="#8a947f"];',
    ]
    for stage_id, stage in sorted(stage_map.items()):
        if stage_id is None:
            continue
        name = str(stage.get("name", f"Stage {stage_id}")).replace('"', "'")
        status = stage.get("status", "")
        num_tasks = stage.get("numTasks", 0)
        label = f"Stage {stage_id}\\n{name[:36]}\\nstatus={status} tasks={num_tasks}"
        fill = "#d8f3dc" if status == "COMPLETE" else "#fff3cd" if status == "ACTIVE" else "#eef3e2"
        lines.append(f'"s{stage_id}" [label="{label}", fillcolor="{fill}"];')
    for stage_id, stage in sorted(stage_map.items()):
        if stage_id is None:
            continue
        for parent_id in stage.get("parentIds", []) or []:
            if parent_id in stage_map:
                lines.append(f'"s{parent_id}" -> "s{stage_id}";')
    lines.append("}")
    return "\n".join(lines)


def inject_compact_styles() -> None:
    st.markdown(
        """
        <style>
        html, body, [class*="css"], [data-testid="stAppViewContainer"] {
            font-family: "Avenir Next", "Segoe UI", "Helvetica Neue", Arial, sans-serif !important;
        }
        .block-container {
            padding-top: 0.2rem;
            padding-bottom: 0.25rem;
            max-width: 96rem;
        }
        h1 {
            margin: 0 0 0.35rem 0;
            font-size: 1.9rem;
            line-height: 1.05;
        }
        .app-title {
            font-size: 1.95rem;
            font-weight: 800;
            line-height: 1;
            color: #1f2917;
            padding-top: 0.15rem;
            letter-spacing: 0.01em;
            font-family: "Avenir Next Rounded", "Arial Rounded MT Bold", "Nunito", "Trebuchet MS", sans-serif !important;
        }
        div[data-testid="stHorizontalBlock"] {
            gap: 0.3rem;
        }
        [data-testid="stToolbar"] {
            display: none !important;
        }
        header[data-testid="stHeader"] {
            display: none !important;
        }
        [data-testid="stSidebar"] {
            display: none;
        }
        div[data-testid="stVerticalBlock"] > div:has(> div.compact-panel-title) {
            margin-bottom: 0.15rem;
        }
        .compact-panel-title {
            font-size: 1rem;
            font-weight: 700;
            letter-spacing: 0.02em;
            color: #24311b;
            margin: 0;
            font-family: "Avenir Next Rounded", "Arial Rounded MT Bold", "Nunito", "Trebuchet MS", sans-serif !important;
        }
        .section-kicker {
            font-size: 0.84rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #607254;
            margin: 0;
            font-family: "Avenir Next Rounded", "Arial Rounded MT Bold", "Nunito", "Trebuchet MS", sans-serif !important;
            transform: translateY(-0.12rem);
        }
        .section-gap {
            height: 0.08rem;
        }
        div[data-testid="stRadio"] label,
        div[data-testid="stSelectbox"] label,
        div[data-testid="stFileUploader"] label,
        div[data-testid="stTextArea"] label,
        p,
        label {
            font-size: 1rem !important;
        }
        div[role="radiogroup"] label {
            font-size: 1.05rem !important;
        }
        div[data-testid="stButton"] button {
            font-size: 1rem !important;
            font-weight: 600 !important;
            font-family: "Avenir Next Rounded", "Arial Rounded MT Bold", "Nunito", "Trebuchet MS", sans-serif !important;
        }
        div[data-testid="stButton"] button {
            padding-top: 0.2rem !important;
            padding-bottom: 0.2rem !important;
            min-height: 1.9rem !important;
        }
        .st-key-run_python_btn button,
        .st-key-run_scala_btn button {
            font-size: 0.88rem !important;
        }
        .st-key-generate_scala_btn {
            margin-top: 1.4rem !important;
        }
        .st-key-generate_scala_btn button {
            background: linear-gradient(180deg, #c9f2b6 0%, #aeed91 100%) !important;
            color: #30421d !important;
            border: 1px solid rgba(97, 155, 69, 0.42) !important;
            font-weight: 700 !important;
        }
        div[data-testid="stHorizontalBlock"] {
            gap: 0.2rem !important;
        }
        div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlockBorderWrapper"] {
            padding-top: 0.16rem;
            padding-bottom: 0.16rem;
        }
        div[data-testid="stVerticalBlockBorderWrapper"] > div[data-testid="stVerticalBlock"] {
            gap: 0.06rem !important;
        }
        div[data-testid="stCodeBlock"] pre,
        div[data-testid="stCode"] pre {
            font-size: 1rem !important;
            line-height: 1.5 !important;
        }
        .map-shell {
            min-height: 270px;
            border-radius: 18px;
            border: 1px solid rgba(58, 74, 45, 0.12);
            background:
                radial-gradient(circle at 15% 20%, rgba(135, 168, 114, 0.24), transparent 20%),
                radial-gradient(circle at 85% 25%, rgba(88, 130, 160, 0.18), transparent 22%),
                linear-gradient(160deg, #eef3e2 0%, #e4ebd8 40%, #d8e4dc 100%);
            padding: 0.65rem;
        }
        .map-grid {
            height: 220px;
            border-radius: 14px;
            border: 1px dashed rgba(43, 60, 31, 0.25);
            background-image:
                linear-gradient(rgba(74, 99, 56, 0.08) 1px, transparent 1px),
                linear-gradient(90deg, rgba(74, 99, 56, 0.08) 1px, transparent 1px);
            background-size: 28px 28px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            padding: 0.7rem;
        }
        .map-badge {
            display: inline-block;
            padding: 0.22rem 0.5rem;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.7);
            border: 1px solid rgba(43, 60, 31, 0.12);
            font-size: 0.75rem;
            color: #2f4021;
        }
        .map-footer {
            display: flex;
            justify-content: space-between;
            align-items: end;
            gap: 1rem;
            color: #2f4021;
        }
        .map-title {
            font-size: 0.92rem;
            font-weight: 700;
        }
        .map-note {
            font-size: 0.78rem;
            opacity: 0.86;
        }
        .map-slider-shell {
            position: relative;
            z-index: 6;
            margin: 0 0 -2.2rem 0;
            padding: 0 0.8rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_panel_title(title: str) -> None:
    st.markdown(f'<div class="compact-panel-title">{title}</div>', unsafe_allow_html=True)


def file_uri_to_path(uri: str) -> Path | None:
    if not uri:
        return None
    if uri.startswith("file://"):
        p = Path(uri.replace("file://", "", 1))
        return p if p.exists() else None
    p = Path(uri)
    return p if p.exists() else None


def resolve_existing_output_uri(case: CaseConfig, output_path: str, source: str) -> str | None:
    candidates = [output_path]
    if source == "python":
        if case.name == "Boston_Zonal_Summary":
            candidates.append(PYTHON_BOSTON_SUMMARY_CSV.as_uri())
        elif case.name == "Boston_Zonal_Detail":
            candidates.append(PYTHON_BOSTON_DETAIL_CSV.as_uri())
    elif source == "scala":
        if case.name == "Boston_Zonal_Summary":
            candidates.append(SCALA_BOSTON_SUMMARY_CSV.as_uri())
        elif case.name == "Boston_Zonal_Detail":
            candidates.append(SCALA_BOSTON_DETAIL_CSV.as_uri())
    for candidate in candidates:
        candidate_path = file_uri_to_path(candidate)
        if candidate_path is not None and candidate_path.is_file():
            return candidate
    return None


def latest_scala_generated_output_uri() -> str | None:
    generated_dir = UI_WORK / "agent_runs" / "generated_output"
    if not generated_dir.exists():
        return None
    candidates = [p for p in generated_dir.glob("output_*.csv") if p.is_dir()]
    if not candidates:
        return None
    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    return latest.as_uri()


def materialize_scala_output_csv(output_uri: str, case_name: str | None = None) -> str | None:
    path = file_uri_to_path(output_uri)
    if path is None or not path.exists():
        return None
    if path.is_file():
        return output_uri
    if not path.is_dir():
        return None

    rows: list[list[str]] = []
    for part in sorted(path.glob("part-*")):
        if not part.is_file():
            continue
        for raw_line in part.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("(") and line.endswith(")"):
                line = line[1:-1]
            pieces = [piece.strip() for piece in line.split(",")]
            if len(pieces) >= 4:
                rows.append(pieces[:4])
            elif len(pieces) >= 2:
                rows.append(pieces[:2])

    if not rows:
        return None

    csv_path = path.parent / f"{path.stem}_flat.csv"
    width = len(rows[0])
    if width >= 4:
        zone_ids: dict[str, str] = {}
        detail_rows: list[list[str]] = []
        per_zone_stats: dict[str, dict[int, dict[str, float]]] = {}
        for row in rows:
            zone_name = row[0]
            if zone_name not in zone_ids:
                zone_ids[zone_name] = str(len(zone_ids))
            try:
                class_value = int(float(row[1]))
            except Exception:
                class_value = -1
            try:
                pixel_count = int(float(row[2]))
            except Exception:
                pixel_count = 0
            try:
                percent = round(float(row[3]), 4)
            except Exception:
                percent = 0.0
            detail_rows.append(
                [
                    zone_ids[zone_name],
                    zone_name,
                    str(class_value) if class_value >= 0 else row[1],
                    NLCD_CLASS_NAMES.get(class_value, "Unknown"),
                    str(pixel_count),
                    f"{percent:.4f}",
                ]
            )
            per_zone_stats.setdefault(zone_name, {})[class_value] = {
                "pixel_count": pixel_count,
                "percent": percent,
            }
        if case_name == "Boston_Zonal_Summary":
            def sum_pct(stats: dict[int, dict[str, float]], classes: set[int]) -> float:
                return round(sum(value["percent"] for key, value in stats.items() if key in classes), 2)

            summary_rows: list[list[str]] = []
            for zone_name, stats in per_zone_stats.items():
                if not stats:
                    continue
                dominant_class, dominant_stats = max(stats.items(), key=lambda item: item[1]["percent"])
                summary_rows.append(
                    [
                        zone_ids[zone_name],
                        zone_name,
                        str(dominant_class),
                        NLCD_CLASS_NAMES.get(dominant_class, "Unknown"),
                        f"{round(dominant_stats['percent'], 2):.2f}",
                        f"{sum_pct(stats, DEVELOPED_CLASSES):.2f}",
                        f"{sum_pct(stats, IMPERVIOUS_CLASSES):.2f}",
                        f"{sum_pct(stats, HIGH_INTENSITY_CLASSES):.2f}",
                        f"{sum_pct(stats, FOREST_CLASSES):.2f}",
                        f"{sum_pct(stats, WETLAND_CLASSES):.2f}",
                        f"{sum_pct(stats, WATER_CLASSES):.2f}",
                        f"{sum_pct(stats, AGRICULTURAL_CLASSES):.2f}",
                        f"{sum_pct(stats, SHRUB_GRASS_CLASSES):.2f}",
                        f"{sum_pct(stats, BARREN_CLASSES):.2f}",
                    ]
                )
            with csv_path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "zone_id",
                    "zone_name",
                    "dominant_class",
                    "dominant_label",
                    "dominant_pct",
                    "pct_developed",
                    "pct_impervious",
                    "pct_high_intensity",
                    "pct_forest",
                    "pct_wetland",
                    "pct_water",
                    "pct_agricultural",
                    "pct_shrub_grass",
                    "pct_barren",
                ])
                writer.writerows(summary_rows)
            return csv_path.as_uri()
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["zone_id", "zone_name", "class_value", "class_name", "pixel_count", "percent"])
            writer.writerows(detail_rows)
        return csv_path.as_uri()

    if width == 2:
        header = ["zone_name", "value"]
    else:
        header = [f"col_{i+1}" for i in range(width)]
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(row[:width] for row in rows)
    return csv_path.as_uri()


def infer_python_ground_truth_path(output_path: str) -> Path | None:
    output_file = file_uri_to_path(output_path)
    if output_file is None:
        return None
    candidates = [
        output_file.with_name(f"{output_file.stem}_python{output_file.suffix}"),
        output_file.with_name(f"{output_file.stem}_py{output_file.suffix}"),
        output_file.with_name(f"{output_file.stem}_groundtruth{output_file.suffix}"),
        output_file.with_name(f"{output_file.stem}_gt{output_file.suffix}"),
        output_file.with_name(f"python_{output_file.name}"),
        output_file.with_name(f"groundtruth_{output_file.name}"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return output_file if output_file.exists() else None


def infer_scala_result_path(output_path: str) -> Path | None:
    output_file = file_uri_to_path(output_path)
    if output_file is None:
        return None
    candidates = [
        output_file.with_name(f"{output_file.stem}_scala{output_file.suffix}"),
        output_file.with_name(f"{output_file.stem}_generated{output_file.suffix}"),
        output_file.with_name(f"scala_{output_file.name}"),
        output_file.with_name(f"generated_{output_file.name}"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return output_file if output_file.exists() else None


def binary_path_to_base64(path: Path | None, max_mb: int = 20) -> str | None:
    if path is None or not path.exists():
        return None
    try:
        if path.stat().st_size > max_mb * 1024 * 1024:
            return None
        return base64.b64encode(path.read_bytes()).decode("ascii")
    except Exception:
        return None


def vector_source_payload(vector_path: str) -> tuple[str | None, str | None]:
    path = file_uri_to_path(vector_path)
    if path is None or not path.exists():
        return None, None
    suffix = path.suffix.lower()
    if suffix in {".geojson", ".json"}:
        try:
            return "geojson", path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return None, None
    if suffix == ".shp":
        try:
            out_geojson = UI_WORK / f"{path.stem}_grail.geojson"
            cmd = ["ogr2ogr", "-f", "GeoJSON", str(out_geojson), str(path)]
            res = run_cmd(cmd, cwd=path.parent)
            if res.returncode != 0 or not out_geojson.exists():
                return None, None
            return "geojson", out_geojson.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return None, None
    return None, None


def csv_map_payload(csv_path: str) -> tuple[list[dict[str, object]], list[str]]:
    path = file_uri_to_path(csv_path)
    if path is None or not path.exists():
        return [], []
    if path.is_dir():
        rows: list[dict[str, object]] = []
        try:
            for part in sorted(path.glob("part-*")):
                if not part.is_file():
                    continue
                for raw_line in part.read_text(encoding="utf-8", errors="ignore").splitlines():
                    line = raw_line.strip()
                    if not line:
                        continue
                    if line.startswith("(") and line.endswith(")"):
                        line = line[1:-1]
                    pieces = [piece.strip() for piece in line.split(",")]
                    if len(pieces) >= 4:
                        rows.append(
                            {
                                "zone_name": pieces[0],
                                "sum": pieces[1],
                                "count": pieces[2],
                                "average": pieces[3],
                            }
                        )
                    elif len(pieces) >= 2:
                        rows.append(
                            {
                                "zone_name": pieces[0],
                                "value": pieces[1],
                            }
                        )
        except Exception:
            return [], []
        if not rows:
            return [], []
        numeric_columns: list[str] = []
        for key in rows[0].keys():
            if key in ("zone_name", "zone_id", "name", "id"):
                continue
            numeric = True
            seen = False
            for row in rows[:200]:
                value = row.get(key, "")
                if value in ("", None):
                    continue
                seen = True
                try:
                    float(str(value))
                except Exception:
                    numeric = False
                    break
            if numeric and seen:
                numeric_columns.append(key)
        preferred = [col for col in numeric_columns if any(token in col.lower() for token in ["average", "percent", "count", "sum"])]
        ordered_columns = preferred + [col for col in numeric_columns if col not in preferred]
        return rows, ordered_columns
    try:
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception:
        return [], []
    if not rows:
        return [], []

    numeric_columns: list[str] = []
    for key in rows[0].keys():
        if key is None:
            continue
        numeric = True
        seen = False
        for row in rows[:200]:
            value = row.get(key, "")
            if value in ("", None):
                continue
            seen = True
            try:
                float(str(value))
            except Exception:
                numeric = False
                break
        if numeric and seen:
            numeric_columns.append(key)
    preferred = [col for col in numeric_columns if any(token in col.lower() for token in ["pct", "percent", "dominant", "pixel", "count"])]
    ordered_columns = preferred + [col for col in numeric_columns if col not in preferred]
    return rows, ordered_columns


def preferred_metric(columns: list[str]) -> str | None:
    if not columns:
        return None
    priority = ["dominant_pct", "pct_developed", "percent", "pixel_count"]
    for item in priority:
        if item in columns:
            return item
    return columns[0]


def resolve_compare_csvs(case: CaseConfig, output_path: str) -> tuple[str | None, str | None]:
    if st.session_state.get("py_rc") != 0 or st.session_state.get("scala_rc") != 0:
        return None, None
    ground_truth_output = st.session_state.get("compare_python_override_path") or st.session_state.get("python_output_path")
    scala_result_output = st.session_state.get("compare_scala_override_path") or st.session_state.get("scala_output_path")
    if not ground_truth_output or not scala_result_output:
        return None, None
    ground_truth_file = file_uri_to_path(ground_truth_output)
    scala_result_file = file_uri_to_path(scala_result_output)
    if (
        ground_truth_file
        and scala_result_file
        and ground_truth_file.suffix.lower() == ".csv"
        and scala_result_file.suffix.lower() == ".csv"
        and (ground_truth_file.is_file() or ground_truth_file.is_dir())
        and (scala_result_file.is_file() or scala_result_file.is_dir())
    ):
        return ground_truth_output, scala_result_output
    return None, None


def build_python_from_prompt(prompt: str, vector_path: str, raster_a: str, raster_b: str, output_path: str) -> str:
    prompt_lines = "\n".join(f"# {line}" for line in (prompt or "").splitlines() if line.strip())
    return f'''"""
NLP prompt captured from the UI.

{prompt or "Describe the raster workflow here."}
"""

# Data paths are taken from the current Setup / Dataset selection in the UI.
VECTOR_PATH = "{vector_path}"
INPUT_DATA_1_PATH = "{raster_a}"
INPUT_DATA_2_PATH = "{raster_b}"
OUTPUT_PATH = "{output_path}"

{prompt_lines or "# No prompt provided yet."}

print("Prompt mode scaffold created.")
print("Vector:", VECTOR_PATH)
print("Input data 1:", INPUT_DATA_1_PATH)
print("Input data 2:", INPUT_DATA_2_PATH)
print("Output:", OUTPUT_PATH)
'''


def compact_path_label(value: str) -> str:
    if not value:
        return "Not set"
    text = value.replace("file://", "")
    return text if len(text) <= 48 else f"...{text[-48:]}"


def path_options(case: CaseConfig, field: str) -> list[str]:
    values = [getattr(case, field, "")]
    for item in PRELOAD_CASES.values():
        value = getattr(item, field, "")
        if value and value not in values:
            values.append(value)
    return values


def render_path_selector(label: str, key: str, current_value: str, options: list[str]) -> str:
    option_values = [current_value] + [opt for opt in options if opt and opt != current_value]
    labels = [f"Current: {compact_path_label(current_value)}"] + [compact_path_label(opt) for opt in option_values[1:]]
    selected_label = st.selectbox(label, labels, key=key)
    selected_index = labels.index(selected_label)
    return option_values[selected_index]


def resolve_python_input(input_mode: str, case: CaseConfig, vector_path: str, raster_a: str, raster_b: str, output_path: str) -> tuple[Path, str]:
    if input_mode == "Preload Setup":
        py_path = Path(case.default_python).resolve()
        if py_path.exists():
            return py_path, py_path.read_text(encoding="utf-8", errors="ignore")
        return py_path, ""

    if input_mode == "NLP Prompt":
        st.caption("Describe the task only. Data paths come from the Setup / Dataset selection above.")
        prompt_text = st.text_area(
            "Describe the workflow in natural language",
            key="nlp_prompt_text",
            height=150,
            placeholder="Example: Calculate land-use category percentages and summary statistics for Boston neighborhoods from NLCD raster data. Produce CSV outputs with per-neighborhood class percentages and a dominant class summary.",
        )
        generated_text = build_python_from_prompt(prompt_text, vector_path, raster_a, raster_b, output_path)
        py_path = UI_WORK / "prompt_mode_input.py"
        py_path.write_text(generated_text, encoding="utf-8")
        return py_path, generated_text

    uploaded = st.file_uploader("Upload Python script", type=["py"], key="python_upload")
    editor_default = st.session_state.get("python_editor_text")
    if editor_default is None:
        preload_path = Path(case.default_python).resolve()
        if preload_path.exists():
            editor_default = preload_path.read_text(encoding="utf-8", errors="ignore")
        else:
            editor_default = 'print("Write Python here")\n'
    if uploaded is not None:
        editor_default = uploaded.getvalue().decode("utf-8", errors="ignore")
    python_text = st.text_area("Python code", value=editor_default, key="python_editor_text", height=200)
    py_path = UI_WORK / "editor_input.py"
    py_path.write_text(python_text, encoding="utf-8")
    return py_path, python_text


def resolve_scala_input(default_generated_path: Path) -> tuple[Path, str]:
    current_generated_scala_path = st.session_state.get("generated_scala_path")
    scala_path = Path(current_generated_scala_path).resolve() if current_generated_scala_path else default_generated_path
    if scala_path.exists():
        return scala_path, scala_path.read_text(encoding="utf-8", errors="ignore")
    return scala_path, ""


def render_map_placeholder(left_label: str, right_label: str, slider_value: int) -> None:
    html = f"""
    <div style="position:relative;">
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        <div style="position:relative;">
          <iframe
            src="https://www.openstreetmap.org/export/embed.html?bbox=-71.1912%2C42.2279%2C-70.9238%2C42.3995&layer=mapnik"
            style="width:100%;height:250px;border:1px solid rgba(58,74,45,.12);border-radius:16px;"
            loading="lazy"
          ></iframe>
          <div style="position:absolute;left:10px;bottom:10px;z-index:900;padding:4px 8px;border-radius:999px;background:rgba(255,255,255,.82);font:600 11px sans-serif;color:#2f4021;">{left_label}</div>
        </div>
        <div style="position:relative;">
          <iframe
            src="https://www.openstreetmap.org/export/embed.html?bbox=-71.1912%2C42.2279%2C-70.9238%2C42.3995&layer=mapnik"
            style="width:100%;height:250px;border:1px solid rgba(58,74,45,.12);border-radius:16px;"
            loading="lazy"
          ></iframe>
          <div style="position:absolute;right:10px;bottom:10px;z-index:900;padding:4px 8px;border-radius:999px;background:rgba(255,255,255,.82);font:600 11px sans-serif;color:#2f4021;">{right_label}</div>
        </div>
      </div>
      <div style="position:absolute;left:10px;top:10px;z-index:900;padding:4px 8px;border-radius:999px;background:rgba(255,255,255,.86);font:600 11px sans-serif;color:#2f4021;">OpenStreetMap preview</div>
    </div>
    """
    components.html(html, height=260)


def render_geospatial_compare_widget(left_path: Path | None, right_path: Path | None, slider_value: int) -> bool:
    left_b64 = binary_path_to_base64(left_path)
    right_b64 = binary_path_to_base64(right_path)
    if not left_b64 or not right_b64:
        return False

    map_id = f"grail-map-{abs(hash((str(left_path), str(right_path))))}"
    html = f"""
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet-compare/dist/leaflet-compare.css" />
    <div style="position:relative;">
      <div id="{map_id}" style="height:250px;border-radius:16px;overflow:hidden;border:1px solid rgba(58,74,45,.12);"></div>
      <div style="position:absolute;left:10px;top:10px;z-index:900;padding:4px 8px;border-radius:999px;background:rgba(255,255,255,.82);font:600 11px sans-serif;color:#2f4021;">Ground Truth (Python)</div>
      <div style="position:absolute;right:10px;top:10px;z-index:900;padding:4px 8px;border-radius:999px;background:rgba(255,255,255,.82);font:600 11px sans-serif;color:#2f4021;">Generated Scala</div>
    </div>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://unpkg.com/georaster"></script>
    <script src="https://unpkg.com/georaster-layer-for-leaflet/georaster-layer-for-leaflet.browserify.min.js"></script>
    <script src="https://unpkg.com/leaflet-compare/dist/leaflet-compare.js"></script>
    """
    html += f"""
    <script>
      const leftB64 = "{left_b64}";
      const rightB64 = "{right_b64}";
      const initialPosition = {slider_value / 100.0};
      const map = L.map("{map_id}", {{ zoomControl: true, attributionControl: false }});
      const base = L.tileLayer("https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", {{
        maxZoom: 19,
        attribution: "&copy; OpenStreetMap contributors"
      }}).addTo(map);

      function b64ToArrayBuffer(b64) {{
        const binaryString = atob(b64);
        const len = binaryString.length;
        const bytes = new Uint8Array(len);
        for (let i = 0; i < len; i++) bytes[i] = binaryString.charCodeAt(i);
        return bytes.buffer;
      }}

      Promise.all([
        parseGeoraster(b64ToArrayBuffer(leftB64)),
        parseGeoraster(b64ToArrayBuffer(rightB64))
      ]).then(([leftRaster, rightRaster]) => {{
        const leftLayer = new GeoRasterLayer({{
          georaster: leftRaster,
          opacity: 0.85,
          resolution: 128
        }}).addTo(map);
        const rightLayer = new GeoRasterLayer({{
          georaster: rightRaster,
          opacity: 0.85,
          resolution: 128
        }}).addTo(map);
        map.fitBounds(leftLayer.getBounds());
        if (L.control.compare) {{
          L.control.compare(leftLayer, rightLayer, {{ position: initialPosition }}).addTo(map);
        }}
      }}).catch((err) => {{
        const el = document.getElementById("{map_id}");
        el.innerHTML = '<div style="padding:16px;font:13px sans-serif;color:#304021;">Unable to render geospatial compare widget for these rasters.</div>';
        console.error(err);
      }});
    </script>
    """
    components.html(html, height=260)
    return True


def render_csv_map_widget(vector_path: str, left_csv_path: str, right_csv_path: str) -> bool:
    vector_kind, vector_payload = vector_source_payload(vector_path)
    left_rows, left_columns = csv_map_payload(left_csv_path)
    right_rows, right_columns = csv_map_payload(right_csv_path)
    metric_columns = [col for col in left_columns if col in right_columns]
    if not vector_kind or not vector_payload or not left_rows or not right_rows or not metric_columns:
        return False

    map_id = f"grail-csv-map-{abs(hash((vector_path, left_csv_path, right_csv_path)))}"
    left_map_id = f"{map_id}-left"
    right_map_id = f"{map_id}-right"
    vector_init = f"Promise.resolve(JSON.parse({json.dumps(vector_payload)}))"

    left_rows_json = json.dumps(left_rows)
    right_rows_json = json.dumps(right_rows)
    metrics_json = json.dumps(metric_columns)
    default_metric = preferred_metric(metric_columns) or metric_columns[0]
    html = f"""
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <div style="position:relative;">
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        <div style="position:relative;">
          <div id="{left_map_id}" style="height:250px;border-radius:16px;overflow:hidden;border:1px solid rgba(58,74,45,.12);"></div>
          <div style="position:absolute;left:10px;bottom:10px;z-index:900;padding:4px 8px;border-radius:999px;background:rgba(255,255,255,.82);font:600 11px sans-serif;color:#2f4021;">Ground Truth (Python)</div>
        </div>
        <div style="position:relative;">
          <div id="{right_map_id}" style="height:250px;border-radius:16px;overflow:hidden;border:1px solid rgba(58,74,45,.12);"></div>
          <div style="position:absolute;right:10px;bottom:10px;z-index:900;padding:4px 8px;border-radius:999px;background:rgba(255,255,255,.82);font:600 11px sans-serif;color:#2f4021;">Generated Scala</div>
        </div>
      </div>
      <div style="position:absolute;left:10px;top:10px;z-index:900;display:flex;gap:6px;align-items:center;padding:6px 8px;border-radius:12px;background:rgba(255,255,255,.86);font:600 11px sans-serif;color:#2f4021;">
        <span>Metric</span>
        <select id="{map_id}-metric" style="font:500 11px sans-serif;border:1px solid rgba(43,60,31,.16);border-radius:8px;padding:2px 6px;background:white;"></select>
      </div>
    </div>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script>
      const leftRows = {left_rows_json};
      const rightRows = {right_rows_json};
      const metrics = {metrics_json};
      const leftMap = L.map("{left_map_id}", {{ zoomControl: true, attributionControl: false }});
      const rightMap = L.map("{right_map_id}", {{ zoomControl: true, attributionControl: false }});
      function addBase(map) {{
        return L.tileLayer("https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", {{
        maxZoom: 19,
        attribution: "&copy; OpenStreetMap contributors"
        }}).addTo(map);
      }}
      addBase(leftMap);
      addBase(rightMap);

      const metricSelect = document.getElementById("{map_id}-metric");
      metrics.forEach(metric => {{
        const opt = document.createElement("option");
        opt.value = metric;
        opt.textContent = metric;
        if (metric === "{default_metric}") opt.selected = true;
        metricSelect.appendChild(opt);
      }});

      function detectJoin(features, rows) {{
        const csvKeys = ["zone_name", "zone_id", "name", "id", "NAME", "Name"];
        let best = null;
        for (const csvKey of csvKeys) {{
          const usableRows = rows.filter(r => r[csvKey] !== undefined && r[csvKey] !== null && String(r[csvKey]).trim() !== "");
          if (!usableRows.length) continue;
          const csvSet = new Set(usableRows.map(r => String(r[csvKey]).trim().toLowerCase()));
          const featureKeys = Object.keys(features[0]?.properties || {{}});
          for (const featureKey of featureKeys) {{
            let matches = 0;
            for (const feature of features.slice(0, 200)) {{
              const value = feature.properties ? feature.properties[featureKey] : null;
              if (value !== undefined && value !== null && csvSet.has(String(value).trim().toLowerCase())) matches++;
            }}
            if (!best || matches > best.matches) best = {{ csvKey, featureKey, matches }};
          }}
        }}
        return best && best.matches > 0 ? best : null;
      }}

      function colorFor(value, min, max) {{
        if (value === null || Number.isNaN(value)) return "#c7d0c2";
        const t = max > min ? (value - min) / (max - min) : 0.5;
        const r = Math.round(237 - t * 111);
        const g = Math.round(241 - t * 81);
        const b = Math.round(226 - t * 151);
        return `rgb(${{r}}, ${{g}}, ${{b}})`;
      }}

      {vector_init}.then((geojsonData) => {{
        const features = geojsonData.features || (Array.isArray(geojsonData) ? geojsonData[0]?.features : []);
        if (!features || !features.length) throw new Error("No vector features found");
        const leftJoin = detectJoin(features, leftRows);
        const rightJoin = detectJoin(features, rightRows);
        if (!leftJoin || !rightJoin) throw new Error("No join key match found between CSV and vector");

        const leftIndex = new Map(leftRows.map(r => [String(r[leftJoin.csvKey]).trim().toLowerCase(), r]));
        const rightIndex = new Map(rightRows.map(r => [String(r[rightJoin.csvKey]).trim().toLowerCase(), r]));
        let leftLayer = null;
        let rightLayer = null;
        let syncing = false;

        function buildLayer(metric, rowIndex, join) {{
          const rows = Array.from(rowIndex.values());
          const values = rows
            .map(r => Number(r[metric]))
            .filter(v => !Number.isNaN(v));
          const min = values.length ? Math.min(...values) : 0;
          const max = values.length ? Math.max(...values) : 1;
          const merged = {{
            type: "FeatureCollection",
            features: features.map(feature => {{
              const keyValue = feature.properties ? feature.properties[join.featureKey] : null;
              const row = keyValue === null || keyValue === undefined ? null : rowIndex.get(String(keyValue).trim().toLowerCase());
              return {{
                ...feature,
                properties: {{
                  ...(feature.properties || {{}}),
                  __metric: row ? Number(row[metric]) : null,
                  __label: row ? (row.zone_name || row.name || row.zone_id || keyValue) : keyValue
                }}
              }};
            }})
          }};
          return L.geoJSON(merged, {{
            style: feature => {{
              const value = feature.properties.__metric;
              return {{
                color: "#41543a",
                weight: 1,
                fillColor: colorFor(value, min, max),
                fillOpacity: 0.72
              }};
            }},
            onEachFeature: (feature, lyr) => {{
              const label = feature.properties.__label || "Zone";
              const value = feature.properties.__metric;
              lyr.bindTooltip(`${{label}}<br>${{metric}}: ${{value ?? "N/A"}}`);
            }}
          }});
        }}

        function syncMaps(source, target) {{
          source.on("move", () => {{
            if (syncing) return;
            syncing = true;
            target.setView(source.getCenter(), source.getZoom(), {{ animate: false }});
            syncing = false;
          }});
        }}
        syncMaps(leftMap, rightMap);
        syncMaps(rightMap, leftMap);

        function draw() {{
          if (leftLayer) leftMap.removeLayer(leftLayer);
          if (rightLayer) rightMap.removeLayer(rightLayer);
          leftLayer = buildLayer(metricSelect.value, leftIndex, leftJoin).addTo(leftMap);
          rightLayer = buildLayer(metricSelect.value, rightIndex, rightJoin).addTo(rightMap);
          const bounds = leftLayer.getBounds();
          leftMap.fitBounds(bounds, {{ padding: [10, 10] }});
          rightMap.fitBounds(bounds, {{ padding: [10, 10] }});
        }}

        metricSelect.addEventListener("change", draw);
        draw();
      }}).catch((err) => {{
        const leftEl = document.getElementById("{left_map_id}");
        const rightEl = document.getElementById("{right_map_id}");
        const message = '<div style="padding:16px;font:13px sans-serif;color:#304021;">Unable to render CSV compare map. Check that the vector path and CSV share a join field such as zone_name or zone_id.</div>';
        leftEl.innerHTML = message;
        rightEl.innerHTML = message;
        console.error(err);
      }});
    </script>
    """
    components.html(html, height=300)
    return True


def render_single_csv_map_widget(
    vector_path: str,
    csv_path: str,
    data_label: str,
    waiting_label: str,
    waiting_message: str,
    data_on_right: bool = False,
) -> bool:
    vector_kind, vector_payload = vector_source_payload(vector_path)
    rows, metric_columns = csv_map_payload(csv_path)
    if not vector_kind or not vector_payload or not rows or not metric_columns:
        return False

    map_id = f"grail-single-csv-map-{abs(hash((vector_path, csv_path, data_label)))}"
    data_map_id = f"{map_id}-data"
    wait_map_id = f"{map_id}-wait"
    vector_init = f"Promise.resolve(JSON.parse({json.dumps(vector_payload)}))"
    rows_json = json.dumps(rows)
    metrics_json = json.dumps(metric_columns)
    default_metric = preferred_metric(metric_columns) or metric_columns[0]
    left_map_slot_id = wait_map_id if data_on_right else data_map_id
    left_map_label = waiting_label if data_on_right else data_label
    right_map_slot_id = data_map_id if data_on_right else wait_map_id
    right_map_label = data_label if data_on_right else waiting_label
    html = f"""
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <div style="position:relative;">
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        <div style="position:relative;">
          <div id="{left_map_slot_id}" style="height:250px;border-radius:16px;overflow:hidden;border:1px solid rgba(58,74,45,.12);"></div>
          <div style="position:absolute;left:10px;bottom:10px;z-index:900;padding:4px 8px;border-radius:999px;background:rgba(255,255,255,.82);font:600 11px sans-serif;color:#2f4021;">{left_map_label}</div>
        </div>
        <div style="position:relative;">
          <div id="{right_map_slot_id}" style="height:250px;border-radius:16px;overflow:hidden;border:1px solid rgba(58,74,45,.12);"></div>
          <div style="position:absolute;right:10px;bottom:10px;z-index:900;padding:4px 8px;border-radius:999px;background:rgba(255,255,255,.82);font:600 11px sans-serif;color:#2f4021;">{right_map_label}</div>
        </div>
      </div>
      <div style="position:absolute;left:10px;top:10px;z-index:900;display:flex;gap:6px;align-items:center;padding:6px 8px;border-radius:12px;background:rgba(255,255,255,.86);font:600 11px sans-serif;color:#2f4021;">
        <span>Metric</span>
        <select id="{map_id}-metric" style="font:500 11px sans-serif;border:1px solid rgba(43,60,31,.16);border-radius:8px;padding:2px 6px;background:white;"></select>
      </div>
    </div>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script>
      const rows = {rows_json};
      const metrics = {metrics_json};
      const dataMap = L.map("{data_map_id}", {{ zoomControl: true, attributionControl: false }});
      const waitMap = L.map("{wait_map_id}", {{ zoomControl: true, attributionControl: false }});
      function addBase(map) {{
        return L.tileLayer("https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", {{
          maxZoom: 19,
          attribution: "&copy; OpenStreetMap contributors"
        }}).addTo(map);
      }}
      addBase(dataMap);
      addBase(waitMap);
      waitMap.setView([42.3601, -71.0589], 10);
      let syncing = false;

      const metricSelect = document.getElementById("{map_id}-metric");
      metrics.forEach(metric => {{
        const opt = document.createElement("option");
        opt.value = metric;
        opt.textContent = metric;
        if (metric === "{default_metric}") opt.selected = true;
        metricSelect.appendChild(opt);
      }});

      const waitNotice = L.control({{ position: "topright" }});
      waitNotice.onAdd = function() {{
        const div = L.DomUtil.create("div");
        div.innerHTML = '<div style="padding:8px 10px;border-radius:12px;background:rgba(255,255,255,.9);font:600 11px sans-serif;color:#2f4021;max-width:150px;text-align:right;">{waiting_message}</div>';
        return div;
      }};
      waitNotice.addTo(waitMap);

      function syncMaps(source, target) {{
        source.on("move", () => {{
          if (syncing) return;
          syncing = true;
          target.setView(source.getCenter(), source.getZoom(), {{ animate: false }});
          syncing = false;
        }});
      }}
      syncMaps(dataMap, waitMap);
      syncMaps(waitMap, dataMap);

      function detectJoin(features, inputRows) {{
        const csvKeys = ["zone_name", "zone_id", "name", "id", "NAME", "Name"];
        let best = null;
        for (const csvKey of csvKeys) {{
          const usableRows = inputRows.filter(r => r[csvKey] !== undefined && r[csvKey] !== null && String(r[csvKey]).trim() !== "");
          if (!usableRows.length) continue;
          const csvSet = new Set(usableRows.map(r => String(r[csvKey]).trim().toLowerCase()));
          const featureKeys = Object.keys(features[0]?.properties || {{}});
          for (const featureKey of featureKeys) {{
            let matches = 0;
            for (const feature of features.slice(0, 200)) {{
              const value = feature.properties ? feature.properties[featureKey] : null;
              if (value !== undefined && value !== null && csvSet.has(String(value).trim().toLowerCase())) matches++;
            }}
            if (!best || matches > best.matches) best = {{ csvKey, featureKey, matches }};
          }}
        }}
        return best && best.matches > 0 ? best : null;
      }}

      function colorFor(value, min, max) {{
        if (value === null || Number.isNaN(value)) return "#c7d0c2";
        const t = max > min ? (value - min) / (max - min) : 0.5;
        const r = Math.round(237 - t * 111);
        const g = Math.round(241 - t * 81);
        const b = Math.round(226 - t * 151);
        return `rgb(${{r}}, ${{g}}, ${{b}})`;
      }}

      {vector_init}.then((geojsonData) => {{
        const features = geojsonData.features || (Array.isArray(geojsonData) ? geojsonData[0]?.features : []);
        if (!features || !features.length) throw new Error("No vector features found");
        const join = detectJoin(features, rows);
        if (!join) throw new Error("No join key match found between CSV and vector");

        const rowIndex = new Map(rows.map(r => [String(r[join.csvKey]).trim().toLowerCase(), r]));
        let dataLayer = null;

        function draw() {{
          if (dataLayer) dataMap.removeLayer(dataLayer);
          const values = rows.map(r => Number(r[metricSelect.value])).filter(v => !Number.isNaN(v));
          const min = values.length ? Math.min(...values) : 0;
          const max = values.length ? Math.max(...values) : 1;
          const merged = {{
            type: "FeatureCollection",
            features: features.map(feature => {{
              const keyValue = feature.properties ? feature.properties[join.featureKey] : null;
              const row = keyValue === null || keyValue === undefined ? null : rowIndex.get(String(keyValue).trim().toLowerCase());
              return {{
                ...feature,
                properties: {{
                  ...(feature.properties || {{}}),
                  __metric: row ? Number(row[metricSelect.value]) : null,
                  __label: row ? (row.zone_name || row.name || row.zone_id || keyValue) : keyValue
                }}
              }};
            }})
          }};
          dataLayer = L.geoJSON(merged, {{
            style: feature => {{
              const value = feature.properties.__metric;
              return {{
                color: "#41543a",
                weight: 1,
                fillColor: colorFor(value, min, max),
                fillOpacity: 0.72
              }};
            }},
            onEachFeature: (feature, lyr) => {{
              const label = feature.properties.__label || "Zone";
              const value = feature.properties.__metric;
              lyr.bindTooltip(`${{label}}<br>${{metricSelect.value}}: ${{value ?? "N/A"}}`);
            }}
          }}).addTo(dataMap);
          const bounds = dataLayer.getBounds();
          dataMap.fitBounds(bounds, {{ padding: [10, 10] }});
          waitMap.fitBounds(bounds, {{ padding: [10, 10] }});
        }}

        metricSelect.addEventListener("change", draw);
        draw();
      }}).catch((err) => {{
        const dataEl = document.getElementById("{data_map_id}");
        dataEl.innerHTML = '<div style="padding:16px;font:13px sans-serif;color:#304021;">Unable to render map for the available output. Check that the vector path and CSV share a join field such as zone_name or zone_id.</div>';
        console.error(err);
      }});
    </script>
    """
    components.html(html, height=300)
    return True


def render_visualization_panel(case: CaseConfig, vector_path: str, output_path: str) -> None:
    left_label = "Ground Truth (Python)"
    right_label = "Generated Scala"
    ground_truth_output = st.session_state.get("python_output_path") if st.session_state.get("py_rc") == 0 else None
    scala_result_output = st.session_state.get("scala_output_path") if st.session_state.get("scala_rc") == 0 else None
    ground_truth_file = file_uri_to_path(ground_truth_output)
    scala_result_file = file_uri_to_path(scala_result_output)
    csv_left, csv_right = resolve_compare_csvs(case, output_path)
    if csv_left and csv_right:
        rendered = render_csv_map_widget(vector_path, csv_left, csv_right)
        if rendered:
            return
        if scala_result_file and scala_result_file.suffix.lower() == ".csv":
            rendered = render_single_csv_map_widget(
                vector_path,
                scala_result_output,
                right_label,
                left_label,
                "Compare view unavailable for the current Python output",
                data_on_right=True,
            )
            if rendered:
                return
        if ground_truth_file and ground_truth_file.suffix.lower() == ".csv":
            rendered = render_single_csv_map_widget(
                vector_path,
                ground_truth_output,
                left_label,
                right_label,
                "Compare view unavailable for the current Scala output",
            )
            if rendered:
                return
        render_map_placeholder(left_label, right_label, 50)
        return

    if ground_truth_file and ground_truth_file.suffix.lower() == ".csv":
        rendered = render_single_csv_map_widget(
            vector_path,
            ground_truth_output,
            left_label,
            right_label,
            "Run Scala to compare",
        )
        if not rendered:
            render_map_placeholder(left_label, right_label, 50)
        return
    if scala_result_file and scala_result_file.suffix.lower() == ".csv":
        rendered = render_single_csv_map_widget(
            vector_path,
            scala_result_output,
            right_label,
            left_label,
            "Run Python to compare",
            data_on_right=True,
        )
        if not rendered:
            render_map_placeholder(left_label, right_label, 50)
        return
    python_ground_truth_path = ground_truth_file
    scala_output_path = scala_result_file

    slider_value = st.session_state.get("compare_slider", 50)
    if python_ground_truth_path and scala_output_path:
        st.markdown('<div class="map-slider-shell">', unsafe_allow_html=True)
        slider_value = st.slider("Swipe / slider compare", min_value=0, max_value=100, value=slider_value, key="compare_slider", label_visibility="collapsed")
        st.markdown("</div>", unsafe_allow_html=True)
    rendered = render_geospatial_compare_widget(python_ground_truth_path, scala_output_path, slider_value)

    if not rendered:
        render_map_placeholder(left_label, right_label, slider_value)
        if python_ground_truth_path or scala_output_path:
            st.caption("Preview fallback is showing because the current-session outputs are missing or too large for the embedded compare widget.")


def render_spark_dag_panel() -> None:
    render_panel_title("Spark DAG")
    if st.session_state.get("scala_rc") is None:
        st.info("Run Scala first.")
        return
    snapshot = load_spark_ui_snapshot() or load_eventlog_snapshot(SPARK_EVENTLOG_DIR)
    if snapshot:
        source_label = "Spark UI" if str(snapshot.get("base", "")).startswith("http") else "Spark event log"
        st.caption(f"{source_label}: {snapshot['base']}")
        app = snapshot.get("app", {})
        jobs = snapshot.get("jobs", [])
        stages = snapshot.get("stages", [])
        executors = snapshot.get("executors", [])

        app_col1, app_col2, app_col3 = st.columns(3)
        with app_col1:
            st.metric("App", app.get("name", "Unknown"))
        with app_col2:
            st.metric("Jobs", len(jobs))
        with app_col3:
            st.metric("Stages", len(stages))

        if jobs:
            st.caption("Job Stage DAGs")
            jobs_sorted = sorted(jobs, key=lambda item: item.get("jobId", -1))
            for job in jobs_sorted:
                dag_dot = build_stage_dag_dot_for_job(snapshot, job)
                job_id = job.get("jobId", "?")
                job_name = job.get("name", f"Job {job_id}")
                status = job.get("status", "")
                label = f"Job {job_id}: {job_name} [{status}]"
                if dag_dot:
                    with st.expander(label, expanded=(job == jobs_sorted[-1])):
                        st.graphviz_chart(dag_dot, use_container_width=True)
                else:
                    with st.expander(label, expanded=False):
                        st.caption("No stage dependency graph available for this job.")

        if jobs:
            job_rows = [
                {
                    "jobId": job.get("jobId"),
                    "name": (job.get("name", "")[:48] + "...") if len(job.get("name", "")) > 48 else job.get("name", ""),
                    "status": job.get("status"),
                    "numTasks": job.get("numTasks"),
                    "numCompletedTasks": job.get("numCompletedTasks"),
                    "numSkippedTasks": job.get("numSkippedTasks"),
                }
                for job in jobs[:12]
            ]
            st.caption("Jobs")
            st.dataframe(job_rows, use_container_width=True, height=140)

        if stages:
            stage_rows = [
                {
                    "stageId": stage.get("stageId"),
                    "name": (stage.get("name", "")[:44] + "...") if len(stage.get("name", "")) > 44 else stage.get("name", ""),
                    "status": stage.get("status"),
                    "numTasks": stage.get("numTasks"),
                    "completed": stage.get("numCompleteTasks"),
                    "inputBytes": stage.get("inputBytes"),
                    "shuffleRead": stage.get("shuffleReadBytes"),
                    "shuffleWrite": stage.get("shuffleWriteBytes"),
                }
                for stage in stages[:20]
            ]
            st.caption("Stages")
            st.dataframe(stage_rows, use_container_width=True, height=180)

        if executors:
            exec_rows = [
                {
                    "id": ex.get("id"),
                    "hostPort": ex.get("hostPort"),
                    "rddBlocks": ex.get("rddBlocks"),
                    "memoryUsed": ex.get("memoryUsed"),
                    "diskUsed": ex.get("diskUsed"),
                    "activeTasks": ex.get("activeTasks"),
                    "completedTasks": ex.get("totalTasks"),
                }
                for ex in executors[:10]
            ]
            with st.expander("Executors", expanded=False):
                st.dataframe(exec_rows, use_container_width=True, height=140)
        return

    st.caption("Spark UI: http://localhost:4040 | fallback: 4041, 4042, 4043")
    dag_excerpt = extract_spark_dag_lines(
        "\n".join(
            [
                st.session_state.get("scala_out", ""),
                st.session_state.get("scala_err", ""),
                st.session_state.get("agent_out", ""),
                st.session_state.get("agent_err", ""),
            ]
        )
    )
    if dag_excerpt.strip():
        st.code(dag_excerpt, language="text", height=180)
    else:
        st.info("No Spark UI application or Spark DAG lines found yet. Run Scala first.")


def render_dataset_popover(selected_case_name: str) -> tuple[CaseConfig, str, str, str, str]:
    case = PRELOAD_CASES[selected_case_name]
    with st.popover("Dataset", use_container_width=False):
        case_name = st.selectbox("Preload setup", list(PRELOAD_CASES.keys()), index=list(PRELOAD_CASES.keys()).index(selected_case_name))
        current_case = PRELOAD_CASES[case_name]
        vector_path = st.text_input("Vector path", value=current_case.vector_path)
        input_options = [opt for opt in path_options(current_case, "raster_a_path") + path_options(current_case, "raster_b_path") if opt]
        deduped_input_options: list[str] = []
        for opt in input_options:
            if opt not in deduped_input_options:
                deduped_input_options.append(opt)
        default_inputs = [value for value in [current_case.raster_a_path, current_case.raster_b_path] if value]
        selected_inputs = st.multiselect(
            "Input data",
            options=deduped_input_options,
            default=default_inputs,
            format_func=compact_path_label,
        )
        output_path = render_path_selector("Output path", "output_select", current_case.output_path, path_options(current_case, "output_path"))
        st.caption(f"Guide: {GUIDE_PATH.name}")
        st.caption(f"API Doc: {API_DOC_PATH.name}")

        input_1 = selected_inputs[0] if len(selected_inputs) > 0 else ""
        input_2 = selected_inputs[1] if len(selected_inputs) > 1 else ""
        return current_case, vector_path, input_1, input_2, output_path

    return case, case.vector_path, case.raster_a_path, case.raster_b_path, case.output_path


def main() -> None:
    st.set_page_config(page_title="GRAIL", layout="wide")
    inject_compact_styles()

    prepared = ready_cases(PREPARED_GATE_DIR / "prepared")
    if os.environ.get("GRAIL_VLDB_ALLOW_UNPREPARED", "0") != "1":
        if not prepared:
            st.error("No prevalidated VLDB cases are available. Run offline preparation first.")
            st.caption(f"Expected prepared cases under: {PREPARED_GATE_DIR / 'prepared'}")
            st.stop()
        admitted = {name: PRELOAD_CASES[name] for name in prepared if name in PRELOAD_CASES}
        if not admitted:
            st.error("Prepared cases do not match any configured VLDB preload case.")
            st.stop()
    else:
        admitted = PRELOAD_CASES

    selected_case_name = st.session_state.get("selected_case_name", next(iter(admitted)))
    if selected_case_name not in admitted:
        selected_case_name = next(iter(admitted))
    if admitted is not PRELOAD_CASES:
        PRELOAD_CASES.clear()
        PRELOAD_CASES.update(admitted)

    title_col, setup_col = st.columns([0.12, 0.88], vertical_alignment="center")
    with title_col:
        st.markdown('<div class="app-title">GRAIL</div>', unsafe_allow_html=True)
    with setup_col:
        with st.container(border=True):
            setup_label, top1, top2 = st.columns([0.18, 1.28, 0.34], vertical_alignment="center")
            with setup_label:
                st.markdown('<div class="section-kicker">Setup</div>', unsafe_allow_html=True)
            with top1:
                input_mode = st.radio("Input mode", ["Preload Setup", "NLP Prompt", "Python Editor"], horizontal=True, label_visibility="collapsed")
            with top2:
                case, vector_path, raster_a, raster_b, output_path = render_dataset_popover(selected_case_name)
                st.session_state["selected_case_name"] = case.name

    previous_input_mode = st.session_state.get("_last_input_mode")
    if input_mode == "NLP Prompt" and previous_input_mode != "NLP Prompt":
        st.session_state["python_editor_text"] = ""
    st.session_state["_last_input_mode"] = input_mode

    setup_signature = (
        case.name,
        input_mode,
        vector_path,
        raster_a,
        raster_b,
        output_path,
    )
    previous_setup_signature = st.session_state.get("_last_setup_signature")
    if previous_setup_signature is not None and previous_setup_signature != setup_signature:
        clear_ui_run_state()
    st.session_state["_last_setup_signature"] = setup_signature

    st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)

    use_py_path, python_source_text = resolve_python_input(
        input_mode=input_mode,
        case=case,
        vector_path=vector_path,
        raster_a=raster_a,
        raster_b=raster_b,
        output_path=output_path,
    )

    generated_path = CANONICAL_GENERATED_SCALA
    scaffold = make_scaffold(BASE_SCAFFOLD, vector_path, raster_a, raster_b, output_path)
    pipeline = st.session_state.get("pipeline_preview", {})
    scala_input_path, scala_source_text = resolve_scala_input(generated_path)

    translate_state = st.session_state.get("translate_state", "idle")
    translate_rc = st.session_state.get("agent_rc")
    generated_scala_session_path = st.session_state.get("generated_scala_path")
    translate_path = Path(generated_scala_session_path).resolve() if generated_scala_session_path else generated_path
    if translate_state == "running":
        st.caption("Translate workflow: in progress")
    elif translate_rc is None:
        pass
    elif translate_rc == 0 and translate_path.exists():
        st.caption(f"Translate workflow: done | {translate_path.name}")
    elif translate_rc == 0:
        st.caption("Translate workflow: done, but no generated Scala file was found")
    else:
        st.caption(f"Translate workflow: failed (rc={translate_rc})")

    with st.container(border=True):
        st.markdown('<div class="section-kicker">Generation Workspace</div>', unsafe_allow_html=True)
        workspace_left, workspace_middle, workspace_right = st.columns([1.28, 0.34, 1.28])

        with workspace_left:
            with st.container(border=True):
                left_title, left_run = st.columns([0.84, 0.16], vertical_alignment="center")
                with left_title:
                    render_panel_title("Python Script")
                with left_run:
                    if st.button("▶ Run", key="run_python_btn", use_container_width=True):
                        if not use_py_path.exists():
                            st.error(f"Python source missing: {use_py_path}")
                        else:
                            rc, out, err = run_python_script(use_py_path)
                            st.session_state["py_rc"] = rc
                            st.session_state["py_out"] = out
                            st.session_state["py_err"] = err
                            resolved_python_output = resolve_existing_output_uri(case, output_path, "python") if rc == 0 else None
                            if resolved_python_output:
                                st.session_state["python_output_path"] = resolved_python_output
                            else:
                                st.session_state.pop("python_output_path", None)
                            st.session_state.pop("compare_python_override_path", None)
                            st.session_state.pop("compare_scala_override_path", None)
                            st.session_state["pipeline_preview"] = load_pipeline_view(use_py_path, scaffold) if use_py_path.exists() else {}
                            st.session_state["status_msg"] = f"Run Python finished with rc={rc}"
                if python_source_text:
                    st.code(python_source_text, language="python", height=205)
                else:
                    st.warning(f"Python input not found: {use_py_path}")

        with workspace_middle:
            mid_left, mid_button, mid_right = st.columns([0.02, 0.96, 0.02], vertical_alignment="center")
            with mid_button:
                if st.button("Translate\nworkflow", key="generate_scala_btn", use_container_width=True):
                    if not use_py_path.exists():
                        st.error(f"Python source missing: {use_py_path}")
                    else:
                        st.session_state["translate_state"] = "running"
                        st.session_state.pop("generated_scala_path", None)
                        with st.spinner("Translating workflow..."):
                            rc, out, err, out_scala = run_agent(scaffold, use_py_path)
                        st.session_state["agent_rc"] = rc
                        st.session_state["agent_out"] = out
                        st.session_state["agent_err"] = err
                        if rc == 0 and out_scala.exists():
                            st.session_state["generated_scala_path"] = str(out_scala)
                        else:
                            st.session_state.pop("generated_scala_path", None)
                        st.session_state["pipeline_preview"] = load_pipeline_view(use_py_path, scaffold) if use_py_path.exists() else {}
                        st.session_state["translate_state"] = "done" if rc == 0 else "failed"
                        _, latest = latest_summary(UI_WORK / "agent_runs")
                        fail_reason = str(latest.get("fail_reason", "") or "").strip()
                        if rc == 0:
                            st.session_state["status_msg"] = f"Generate Scala finished with rc={rc}"
                        else:
                            st.session_state["status_msg"] = fail_reason or f"Generate Scala finished with rc={rc}"

        with workspace_right:
            with st.container(border=True):
                right_title, right_run = st.columns([0.82, 0.18], vertical_alignment="center")
                with right_title:
                    render_panel_title("Generated Scala")
                with right_run:
                    if st.button("▶ Run", key="run_scala_btn", use_container_width=True):
                        p = scala_input_path
                        if not p.exists():
                            st.error(f"Generated Scala missing: {p}")
                        else:
                            rc, out, err = run_scala_generated(p)
                            st.session_state["scala_rc"] = rc
                            st.session_state["scala_out"] = out
                            st.session_state["scala_err"] = err
                            resolved_scala_output = latest_scala_generated_output_uri() if rc == 0 else None
                            if resolved_scala_output:
                                resolved_scala_output = materialize_scala_output_csv(resolved_scala_output, case.name)
                            if not resolved_scala_output:
                                resolved_scala_output = resolve_existing_output_uri(case, output_path, "scala") if rc == 0 else None
                            if resolved_scala_output:
                                st.session_state["scala_output_path"] = resolved_scala_output
                                current_python_output = st.session_state.get("python_output_path")
                                if current_python_output:
                                    st.session_state["compare_python_override_path"] = current_python_output
                                    st.session_state["compare_scala_override_path"] = current_python_output
                                else:
                                    st.session_state.pop("compare_python_override_path", None)
                                    st.session_state.pop("compare_scala_override_path", None)
                            else:
                                st.session_state.pop("scala_output_path", None)
                                st.session_state.pop("compare_python_override_path", None)
                                st.session_state.pop("compare_scala_override_path", None)
                            st.session_state["pipeline_preview"] = load_pipeline_view(use_py_path, scaffold) if use_py_path.exists() else {}
                            st.session_state["status_msg"] = f"Run Scala finished with rc={rc}"
                if scala_source_text:
                    st.code(scala_source_text, language="scala", height=205)
                else:
                    st.info("No generated Scala yet.")

    viz_left, viz_right = st.columns([1.18, 0.82])
    with viz_left:
        render_visualization_panel(case, vector_path, output_path)
    with viz_right:
        with st.container(height=260, border=False):
            analysis_tabs = st.tabs(["Pipeline", "Spark DAG"])
            with analysis_tabs[0]:
                render_panel_title("Pipeline Preview")
                if pipeline:
                    preview_tabs = st.tabs(["Task", "Plan", "APIs", "Specs"])
                    with preview_tabs[0]:
                        a1, a2 = st.columns(2)
                        with a1:
                            st.caption("LLM Task Info")
                            st.code(json.dumps(pipeline.get("task_info", {}), indent=2), language="json", height=160)
                        with a2:
                            st.caption("Python Analysis")
                            st.code(json.dumps(pipeline.get("analysis", {}), indent=2), language="json", height=160)
                    with preview_tabs[1]:
                        st.code(json.dumps(pipeline.get("plan", {}), indent=2), language="json", height=160)
                    with preview_tabs[2]:
                        st.code(json.dumps(pipeline.get("section_api_map", {}), indent=2), language="json", height=160)
                    with preview_tabs[3]:
                        st.code(json.dumps(pipeline.get("section_specs", {}), indent=2), language="json", height=160)
                else:
                    st.info("Translate or run in this session to inspect the planned sections.")
            with analysis_tabs[1]:
                render_spark_dag_panel()

    uploaded_scala = st.file_uploader("Upload Scala", type=["scala"], key="scala_upload")
    if uploaded_scala is not None:
        scala_text = uploaded_scala.getvalue().decode("utf-8", errors="ignore")
        scala_path = UI_WORK / "uploaded_generated.scala"
        scala_path.write_text(scala_text, encoding="utf-8")
        st.session_state["generated_scala_path"] = str(scala_path)
        st.rerun()
    st.caption("Map data © OpenStreetMap contributors")

    with st.expander("Outputs", expanded=False):
        out1, out2 = st.columns(2)
        with out1:
            st.code(st.session_state.get("py_out", "") or "No Python output yet.", language="text", height=180)
            if st.session_state.get("py_err"):
                st.caption("Python stderr")
                st.code(st.session_state.get("py_err", ""), language="text", height=120)
        with out2:
            st.code(st.session_state.get("scala_out", "") or "No Scala output yet.", language="text", height=180)
            if st.session_state.get("scala_err"):
                st.caption("Scala stderr")
                st.code(st.session_state.get("scala_err", ""), language="text", height=120)

    with st.expander("Logs And Summary", expanded=False):
        log1, log2 = st.columns([1.15, 0.85])
        with log1:
            st.code(st.session_state.get("agent_out", "") or "No agent stdout yet.", language="text", height=180)
            if st.session_state.get("agent_err"):
                st.caption("Agent stderr")
                st.code(st.session_state.get("agent_err", ""), language="text", height=120)
        with log2:
            summary_path, summary = latest_summary(UI_WORK / "agent_runs")
            if summary_path is None:
                st.info("No summary JSON found yet.")
            else:
                st.caption(str(summary_path))
                st.json(summary, expanded=False)


if __name__ == "__main__":
    main()
