# run_account_health_agent.py
from __future__ import annotations

import argparse
import os
from datetime import datetime

from reader_databricks_health import load_databricks_context
from rules_engine_health import evaluate_all
from writer_account_health import write_account_health_output


def main():
    parser = argparse.ArgumentParser(description="CoE Account Health Agent")
    parser.add_argument("--export", required=True, help="Path to Databricks export .xlsx")
    parser.add_argument("--template", required=True, help="Path to CoE_Account_Health_Analysis_Templates.xlsm")
    args = parser.parse_args()

    export_path = args.export
    template_path = args.template

    if not os.path.exists(export_path):
        raise FileNotFoundError(f"Export not found: {export_path}")

    if not os.path.exists(template_path):
        raise FileNotFoundError(f"Template not found: {template_path}")

    # Load Databricks context
    ctx = load_databricks_context(export_path)

    # Run evaluation engine
    results, ctx = evaluate_all(ctx)

not_eval = sorted([
    cid for cid, res in results.items()
    if isinstance(res.what_we_saw, str) and "not evaluated" in res.what_we_saw.lower()
])

if not_eval:
    print(f"NOT_EVALUATED_CONTROLS: {', '.join(not_eval)}")

    # ------------------------------------------------------------------
    # Unique filename generation (prevents cached / stale download links)
    # ------------------------------------------------------------------

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    filename = f"{ctx.hash_name} - Account Health Analysis - {ts}.xlsm"

    out_path = os.path.join("/mnt/data", filename)

    # Ensure directory exists
    os.makedirs("/mnt/data", exist_ok=True)

    # Write output workbook
    write_account_health_output(
        template_path=template_path,
        output_path=out_path,
        ctx=ctx,
        results=results,
    )

    # ------------------------------------------------------------------
    # Sanity check (prevents empty / broken files)
    # ------------------------------------------------------------------

    if (not os.path.exists(out_path)) or os.path.getsize(out_path) < 50000:
        raise RuntimeError(f"Output file missing or too small: {out_path}")

    # ------------------------------------------------------------------
    # Print output info
    # ------------------------------------------------------------------

    print("DONE")
    print(f"Output file: {out_path}")
    print(f"Account: {ctx.hash_name}")
    print(f"Window: {ctx.window_str}")
    print(f"Downloaded: {ctx.downloaded_dt}")

    # This line is critical for the GPT to generate a reliable download link
    print(f"SANDBOX_LINK: sandbox:{out_path}")


if __name__ == "__main__":
    main()
