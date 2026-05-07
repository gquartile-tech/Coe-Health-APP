"""
CoE Full Analysis Tool — Flask backend
Single upload → 4 outputs: Framework, Account Health, Account Mastery, Strategy
Run:  python app.py
Open: http://127.0.0.1:8500
"""

from __future__ import annotations

import gc
import os
import re
import sys
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from flask import Flask, Response, jsonify, render_template, request
from werkzeug.utils import secure_filename

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent.resolve()
UPLOAD_DIR = BASE_DIR / "uploads"
OUTPUT_DIR = BASE_DIR / "outputs"

TEMPLATES = {
    "framework": BASE_DIR / "templates" / "CoE_Framework_Analysis_Templates.xlsm",
    "health":    BASE_DIR / "templates" / "CoE_Account_Health_Analysis_Templates.xlsm",
    "mastery":   BASE_DIR / "templates" / "CoE_Account_Mastery_Analysis_Templates.xlsm",
    "strategy":  BASE_DIR / "templates" / "CoE_Account_Strategy_Analysis_Templates_V2.xlsm",
}

UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

sys.path.insert(0, str(BASE_DIR))

MIN_OUTPUT_BYTES = 5_000

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024


def _safe_fn(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r'[^a-zA-Z0-9 \-_]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name or "UNKNOWN_ACCOUNT"


# ── Agent runners ─────────────────────────────────────────────────────────────

def run_framework(input_path: str) -> dict:
    from reader_databricks import load_databricks_export
    from rules_engine import evaluate_all
    from writer_framework import write_results_to_template

    tpl = TEMPLATES["framework"]
    if not tpl.exists():
        raise FileNotFoundError(f"Framework template not found: {tpl}")

    ctx = load_databricks_export(input_path)
    hash_name = getattr(ctx, "hash_name", "") or getattr(ctx, "account_name", "") or "UNKNOWN_ACCOUNT"
    safe_hash = _safe_fn(hash_name)
    results = evaluate_all(ctx)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"{safe_hash} - Framework Analysis - {ts}.xlsm"
    fpath = OUTPUT_DIR / fname

    write_results_to_template(
        template_path=str(tpl),
        output_path=str(fpath),
        results=results,
        ctx=ctx,
        sheet_analysis="Framework_Analysis",
        sheet_reference="Framework_Reference",
    )

    size = fpath.stat().st_size if fpath.exists() else 0
    if not fpath.exists() or size < MIN_OUTPUT_BYTES:
        raise RuntimeError(f"Output too small ({size} bytes)")

    return {
        "label":     "Framework Analysis",
        "filename":  fname,
        "ok":        sum(1 for r in results.values() if r.status == "OK"),
        "flag":      sum(1 for r in results.values() if r.status == "FLAG"),
        "partial":   sum(1 for r in results.values() if r.status == "PARTIAL"),
    }


def run_health(input_path: str) -> dict:
    from reader_databricks_health import load_databricks_context
    from rules_engine_health import evaluate_all
    from writer_account_health import write_account_health_output

    tpl = TEMPLATES["health"]
    if not tpl.exists():
        raise FileNotFoundError(f"Health template not found: {tpl}")

    ctx = load_databricks_context(input_path)
    hash_name = getattr(ctx, "hash_name", "") or "UNKNOWN_ACCOUNT"
    safe_hash = _safe_fn(hash_name)
    results, ctx = evaluate_all(ctx)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"{safe_hash} - Account Health Analysis - {ts}.xlsm"
    fpath = OUTPUT_DIR / fname

    write_account_health_output(
        template_path=str(tpl),
        output_path=str(fpath),
        ctx=ctx,
        results=results,
    )

    size = fpath.stat().st_size if fpath.exists() else 0
    if not fpath.exists() or size < MIN_OUTPUT_BYTES:
        raise RuntimeError(f"Output too small ({size} bytes)")

    return {
        "label":   "Account Health Analysis",
        "filename": fname,
        "ok":      sum(1 for r in results.values() if r.status == "OK"),
        "flag":    sum(1 for r in results.values() if r.status == "FLAG"),
        "partial": sum(1 for r in results.values() if r.status == "PARTIAL"),
    }


def run_mastery(input_path: str) -> dict:
    from reader_databricks_mastery import load_databricks_context
    from rules_engine_mastery import evaluate_all, build_summary, compute_score
    from writer_account_mastery import write_mastery_output

    tpl = TEMPLATES["mastery"]
    if not tpl.exists():
        raise FileNotFoundError(f"Mastery template not found: {tpl}")

    ctx = load_databricks_context(input_path)
    hash_name = getattr(ctx, "hash_name", "") or "UNKNOWN_ACCOUNT"
    safe_hash = _safe_fn(hash_name)
    results = evaluate_all(ctx)
    summary = build_summary(ctx, results)
    penalty, score, grade, findings = compute_score(results)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"{safe_hash} - Account Mastery Analysis - {ts}.xlsm"
    fpath = OUTPUT_DIR / fname

    write_mastery_output(
        template_path=str(tpl),
        output_path=str(fpath),
        summary=summary,
        results=results,
        penalty=penalty,
        score=score,
        grade=grade,
        findings=findings,
        ctx=ctx,
    )

    size = fpath.stat().st_size if fpath.exists() else 0
    if not fpath.exists() or size < MIN_OUTPUT_BYTES:
        raise RuntimeError(f"Output too small ({size} bytes)")

    return {
        "label":   "Account Mastery Analysis",
        "filename": fname,
        "score":   round(score, 1),
        "grade":   grade,
        "ok":      sum(1 for r in results.values() if r.status == "OK"),
        "flag":    sum(1 for r in results.values() if r.status == "FLAG"),
        "partial": sum(1 for r in results.values() if r.status == "PARTIAL"),
    }


def run_strategy(input_path: str) -> dict:
    from writer_strategy import write_strategy

    tpl = TEMPLATES["strategy"]
    if not tpl.exists():
        raise FileNotFoundError(f"Strategy template not found: {tpl}")

    result_path = write_strategy(input_path, str(tpl), str(OUTPUT_DIR))
    fpath = Path(result_path)

    size = fpath.stat().st_size if fpath.exists() else 0
    if not fpath.exists() or size < MIN_OUTPUT_BYTES:
        raise RuntimeError(f"Output too small ({size} bytes)")

    return {
        "label":    "Account Strategy Analysis",
        "filename": fpath.name,
    }


AGENTS = {
    "framework": run_framework,
    "health":    run_health,
    "mastery":   run_mastery,
    "strategy":  run_strategy,
}


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/analyze", methods=["POST"])
def analyze():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded."}), 400
    uploaded = request.files["file"]
    if not uploaded.filename:
        return jsonify({"error": "No file selected."}), 400
    _, ext = os.path.splitext(uploaded.filename.lower())
    if ext not in {".xlsx", ".xlsm"}:
        return jsonify({"error": "Only .xlsx or .xlsm files accepted."}), 400

    safe_name = secure_filename(uploaded.filename)
    if not safe_name:
        # Fallback: werkzeug strips all chars from non-ASCII filenames
        safe_name = f"upload_{uuid.uuid4().hex}{ext}"

    input_path = str(UPLOAD_DIR / safe_name)

    try:
        uploaded.save(input_path)

        agent_results = {}

        # Run all 4 agents — each independently, collect success/error per agent
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(fn, input_path): key for key, fn in AGENTS.items()}
            for future in as_completed(futures):
                key = futures[future]
                try:
                    agent_results[key] = {"status": "ok", **future.result()}
                except Exception as e:
                    traceback.print_exc()
                    agent_results[key] = {
                        "status": "error",
                        "label":  key.capitalize(),
                        "error":  str(e),
                    }

    finally:
        try:
            os.remove(input_path)
        except Exception:
            pass
        gc.collect()

    # Always return 200 — let the frontend handle per-agent status
    return jsonify({"agents": agent_results})


@app.route("/download/<path:filename>")
def download(filename):
    from urllib.parse import unquote
    from flask import send_file as _send_file
    filename = unquote(filename)
    p = OUTPUT_DIR / filename

    if not p.exists():
        return f"File not found: {filename}", 404

    return _send_file(
        str(p),
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.ms-excel.sheet.macroEnabled.12",
    )


@app.route("/healthcheck")
def healthcheck():
    missing = [k for k, p in TEMPLATES.items() if not p.exists()]
    ok = len(missing) == 0
    return jsonify({
        "status":            "ok" if ok else "degraded",
        "missing_templates": missing,
    }), 200 if ok else 503


@app.route("/favicon.ico")
def favicon():
    return "", 204


if __name__ == "__main__":
    print("\n  CoE Full Analysis Tool")
    print("  ─────────────────────────────────────────────────")
    for k, p in TEMPLATES.items():
        print(f"  [{k:10s}] {'✓' if p.exists() else '✗ MISSING'} {p.name}")
    print("  Open → http://127.0.0.1:8500\n")
    app.run(host="127.0.0.1", port=8500, debug=True)
