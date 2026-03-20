"""
CoE Account Health Analysis Tool — Flask backend
Run:  py app.py
Open: http://127.0.0.1:8502
"""

from __future__ import annotations

import os
import sys
import traceback
import re
import gc
from datetime import datetime
from pathlib import Path

from flask import Flask, request, jsonify, send_file, render_template, Response
from werkzeug.utils import secure_filename

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).parent.resolve()
UPLOAD_DIR    = BASE_DIR / "uploads"
OUTPUT_DIR    = BASE_DIR / "outputs"
TEMPLATE_FILE = BASE_DIR / "CoE_Account_Health_Analysis_Templates.xlsm"

UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Import analysis modules ───────────────────────────────────────────────────
sys.path.insert(0, str(BASE_DIR))

MIN_OUTPUT_BYTES = 5_000   # lowered — xlsm can be small

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024


def _safe_fn(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r'[^a-zA-Z0-9 \-_]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name or "UNKNOWN_ACCOUNT"


def run_full_analysis(input_path: str) -> dict:
    from reader_databricks_health import load_databricks_context
    from rules_engine_health import evaluate_all
    from writer_account_health import write_account_health_output

    if not TEMPLATE_FILE.exists():
        raise FileNotFoundError(f"Template not found: {TEMPLATE_FILE}")

    ctx = load_databricks_context(input_path)
    hash_name = getattr(ctx, "hash_name", "") or getattr(ctx, "account_name", "") or "UNKNOWN_ACCOUNT"
    safe_hash = _safe_fn(hash_name)

    results, ctx = evaluate_all(ctx)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    download_name = f"{safe_hash} - Account Health Analysis - {ts}.xlsm"
    download_path = OUTPUT_DIR / download_name

    write_account_health_output(
        template_path=str(TEMPLATE_FILE),
        output_path=str(download_path),
        ctx=ctx,
        results=results,
    )

    size = download_path.stat().st_size if download_path.exists() else 0
    print(f"  Output written: {download_path} ({size} bytes)")

    if not download_path.exists() or size < MIN_OUTPUT_BYTES:
        raise RuntimeError(f"Output file missing or too small ({size} bytes).")

    ok_count      = sum(1 for r in results.values() if r.status == "OK")
    flag_count    = sum(1 for r in results.values() if r.status == "FLAG")
    partial_count = sum(1 for r in results.values() if r.status == "PARTIAL")

    del ctx, results
    gc.collect()

    return {
        "download_filename": download_name,
        "account":           hash_name,
        "window":            getattr(ctx, "window_str", ""),
        "ref_date":          str(getattr(ctx, "ref_date", "") or ""),
        "downloaded":        str(getattr(ctx, "downloaded_dt", "") or ""),
        "primary_kpi":       getattr(ctx, "primary_kpi", "ACOS"),
        "acos_constraint":   f"{ctx.acos_constraint*100:.1f}%" if ctx.acos_constraint else "NOT FOUND",
        "tacos_constraint":  f"{ctx.tacos_constraint*100:.1f}%" if ctx.tacos_constraint else "NOT FOUND",
        "ok":                ok_count,
        "flag":              flag_count,
        "partial":           partial_count,
        "flag_ids":          [cid for cid, r in results.items() if r.status == "FLAG"],
        "partial_ids":       [cid for cid, r in results.items() if r.status == "PARTIAL"],
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

    safe_name  = secure_filename(uploaded.filename)
    input_path = str(UPLOAD_DIR / safe_name)
    uploaded.save(input_path)

    try:
        info = run_full_analysis(input_path)
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"Analysis failed: {e}"}), 500
    finally:
        try:
            os.remove(input_path)
        except Exception:
            pass
        gc.collect()

    info["download_url"] = f"/download/{info['download_filename']}"
    return jsonify(info)


@app.route("/download/<path:filename>")
def download(filename):
    from urllib.parse import unquote
    filename = unquote(filename)
    p = OUTPUT_DIR / filename

    # Fallback: serve the most recent .xlsm if exact name not found
    if not p.exists():
        xlsm_files = sorted(OUTPUT_DIR.glob("*.xlsm"), key=lambda f: f.stat().st_mtime, reverse=True)
        if xlsm_files:
            p = xlsm_files[0]
            filename = p.name
            print(f"  Fallback download: {filename}")
        else:
            return f"No output files found in {OUTPUT_DIR}", 404

    print(f"  Serving download: {p} ({p.stat().st_size} bytes)")

    # Read file into memory and send — avoids any path/handle issues on Windows
    data = p.read_bytes()
    return Response(
        data,
        mimetype="application/vnd.ms-excel.sheet.macroEnabled.12",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(data)),
        }
    )


@app.route("/favicon.ico")
def favicon():
    return "", 204


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n  CoE Account Health Analysis Tool")
    print("  ─────────────────────────────────────────────────")
    print(f"  Template : {TEMPLATE_FILE}")
    print(f"  Template exists: {TEMPLATE_FILE.exists()}")
    print(f"  Outputs  : {OUTPUT_DIR}")
    print("  Open → http://127.0.0.1:8502\n")
    app.run(host="127.0.0.1", port=8502, debug=True)
