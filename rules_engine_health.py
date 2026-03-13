from __future__ import annotations

from dataclasses import replace
from datetime import date
from typing import Dict, Optional, List, Tuple
import re

import pandas as pd

import config_health as cfg
from reader_databricks_health import DatabricksContext, get_dataset


# -------------------------
# Result helpers
# -------------------------

def ok(what: str = "", why: str = "", src: str = "", note: str = "") -> cfg.ControlResult:
    return cfg.ControlResult(
        status=cfg.STATUS_OK,
        what_we_saw=what,
        why_it_matters=why,
        data_source=src,
        note=note,
    )

def partial(what: str, why: str = "", src: str = "", note: str = "") -> cfg.ControlResult:
    if not what:
        what = "Observed: Partial signal detected."
    return cfg.ControlResult(
        status=cfg.STATUS_PARTIAL,
        what_we_saw=what,
        why_it_matters=why,
        data_source=src,
        note=note,
    )

def flag(what: str, why: str = "", src: str = "", note: str = "") -> cfg.ControlResult:
    if not what:
        raise ValueError("FLAG requires a non-empty what_we_saw.")
    return cfg.ControlResult(
        status=cfg.STATUS_FLAG,
        what_we_saw=what,
        why_it_matters=why,
        data_source=src,
        note=note,
    )


# -------------------------
# Parsing helpers
# -------------------------

def _to_float(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)) and not pd.isna(x):
        return float(x)
    s = str(x).strip()
    if not s or s.lower() in ("nan", "none", "null", "-"):
        return None
    s = s.replace("$", "").replace("€", "").replace("£", "")
    s = s.replace(",", "").replace(" ", "")
    if s.endswith("%"):
        try:
            return float(s[:-1]) / 100.0
        except Exception:
            return None
    m = re.match(r"^([0-9]*\.?[0-9]+)k$", s, flags=re.I)
    if m:
        try:
            return float(m.group(1)) * 1000.0
        except Exception:
            return None
    try:
        return float(s)
    except Exception:
        return None

def _pct_str(x: float, decimals: int = 1) -> str:
    return f"{x*100:.{decimals}f}%"

def _money_str(x: float) -> str:
    return f"{x:,.0f}"

def _excel_row_to_df_index(excel_row: int) -> int:
    # header=5 => df row 0 is Excel row 7
    return excel_row - 7

def _col_letter_to_zero_index(letter: str) -> int:
    letter = letter.upper().strip()
    n = 0
    for ch in letter:
        n = n * 26 + (ord(ch) - ord('A') + 1)
    return n - 1

def _read_cell_by_pos(df: pd.DataFrame, excel_col_letter: str, excel_row: int) -> Optional[float]:
    if df is None or df.empty:
        return None
    r = _excel_row_to_df_index(excel_row)
    c = _col_letter_to_zero_index(excel_col_letter)
    if r < 0 or c < 0 or r >= len(df.index) or c >= len(df.columns):
        return None
    return _to_float(df.iloc[r, c])

def _read_str_cell_by_pos(df: pd.DataFrame, excel_col_letter: str, excel_row: int) -> str:
    if df is None or df.empty:
        return ""
    r = _excel_row_to_df_index(excel_row)
    c = _col_letter_to_zero_index(excel_col_letter)
    if r < 0 or c < 0 or r >= len(df.index) or c >= len(df.columns):
        return ""
    v = df.iloc[r, c]
    return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v).strip()


def _parse_months_from_text(text: str) -> set[int]:
    if not text:
        return set()
    t = text.lower()
    q_map = {"q1": {1, 2, 3}, "q2": {4, 5, 6}, "q3": {7, 8, 9}, "q4": {10, 11, 12}}
    months = set()
    for q, ms in q_map.items():
        if q in t:
            months |= ms

    name_map = {
        "jan": 1, "january": 1,
        "feb": 2, "february": 2,
        "mar": 3, "march": 3,
        "apr": 4, "april": 4,
        "may": 5,
        "jun": 6, "june": 6,
        "jul": 7, "july": 7,
        "aug": 8, "august": 8,
        "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10,
        "nov": 11, "november": 11,
        "dec": 12, "december": 12,
    }

    for m in re.finditer(r"\b(1[0-2]|[1-9])\s*-\s*(1[0-2]|[1-9])\b", t):
        a = int(m.group(1)); b = int(m.group(2))
        if a <= b:
            months |= set(range(a, b + 1))

    for m in re.finditer(r"\b([a-z]{3,9})\s*-\s*([a-z]{3,9})\b", t):
        a = name_map.get(m.group(1)); b = name_map.get(m.group(2))
        if a and b and a <= b:
            months |= set(range(a, b + 1))

    for k, v in name_map.items():
        if re.search(rf"\b{k}\b", t):
            months.add(v)

    if "prime day" in t and 7 not in months:
        months.add(7)

    return {m for m in months if 1 <= m <= 12}


def _extract_budget_from_text(text: str) -> Optional[float]:
    if not text:
        return None
    t = text.lower()
    if not any(k in t for k in ["budget", "spend target", "budget cap", "monthly budget", "spend"]):
        return None
    patterns = [
        r"(budget|monthly budget|budget cap|spend target)\s*[:=]?\s*\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]*\.?[0-9]+k?|[0-9]+)",
    ]
    for pat in patterns:
        m = re.search(pat, t, flags=re.I)
        if m:
            return _to_float(m.group(2).strip())
    return None


# -------------------------
# Month helpers (for MoM/QoQ and avg 3M)
# -------------------------

def _latest_full_month_anchor(ctx: DatabricksContext) -> Optional[pd.Timestamp]:
    """
    Always anchor to the LAST fully completed month.
    Never use the current (potentially partial) month.
    """
    if not ctx.window_end:
        return None

    we = pd.Timestamp(ctx.window_end)
    month_start = pd.Timestamp(year=we.year, month=we.month, day=1)

    # Always take the prior month as the "latest full month"
    return month_start - pd.offsets.MonthBegin(1)

def _get_last_n_full_month_rows(df_monthly: pd.DataFrame, ctx: DatabricksContext, n: int, month_col_index: int = 0) -> Optional[pd.DataFrame]:
    if df_monthly is None or df_monthly.empty:
        return None
    months = pd.to_datetime(df_monthly.iloc[:, month_col_index], errors="coerce")
    tmp = df_monthly.copy()
    tmp["_month"] = months.dt.to_period("M").dt.to_timestamp()

    anchor = _latest_full_month_anchor(ctx)
    if anchor is None:
        return None

    tmp = tmp[tmp["_month"] <= anchor].sort_values("_month")
    if tmp.empty:
        return None
    return tmp.tail(n)

def _apply_seasonality_thresholds(ctx: DatabricksContext, months_involved: List[pd.Timestamp], base_flag: float, base_partial: float) -> Tuple[float, float]:
    """
    Adjust thresholds for MoM/QoQ only when months overlap declared seasonality.
    """
    if not ctx.season_months:
        return (base_flag, base_partial)
    for m in months_involved:
        if int(m.month) in ctx.season_months:
            return (-0.15, -0.10)
    return (base_flag, base_partial)


# -------------------------
# Why-it-matters helpers (Primary KPI emphasis)
# -------------------------

def _primary_kpi_tag(ctx: DatabricksContext) -> str:
    return f"Primary KPI for this account: {ctx.primary_kpi}."

def _why_constraint_metric(ctx: DatabricksContext, metric: str) -> str:
    if metric.upper() == ctx.primary_kpi.upper():
        return f"{_primary_kpi_tag(ctx)} Exceeding the {metric} constraint directly increases profitability risk and limits scaling."
    return f"{_primary_kpi_tag(ctx)} This metric is secondary for governance but still indicates efficiency pressure if it breaches constraints."

def _why_trend_metric(metric: str) -> str:
    return f"A sustained negative trend in {metric} indicates performance deterioration that can restrict scalability."

def _why_benchmark(metric: str, direction: str) -> str:
    if direction == "higher_worse":
        return f"Being materially higher than category benchmark in {metric} signals weaker competitive efficiency."
    if direction == "lower_worse":
        return f"Being materially below category benchmark in {metric} signals weaker competitive effectiveness."
    return f"Large deviations from the category benchmark in {metric} signal a competitive positioning gap."


# -------------------------
# Context pre-read (CS repo)
# -------------------------

def hydrate_constraints_and_context(ctx: DatabricksContext) -> DatabricksContext:
    df_cs = get_dataset(ctx, "CS_REPO")
    if df_cs is None or df_cs.empty:
        return ctx

    primary = _read_str_cell_by_pos(df_cs, "BW", 7).upper()
    if not primary:
        primary = "ACOS"
    if primary not in ("ACOS", "TACOS", "ROAS"):
        primary = "ACOS"

    acos_c = _read_cell_by_pos(df_cs, "O", 7)
    tacos_c = _read_cell_by_pos(df_cs, "AX", 7)
    mrr = _read_cell_by_pos(df_cs, "AG", 7)

    am7 = _read_str_cell_by_pos(df_cs, "AM", 7)
    budget_target = _extract_budget_from_text(am7)
    season_months = _parse_months_from_text(am7)

    def norm_pct(v: Optional[float]) -> Optional[float]:
        if v is None:
            return None
        if v <= 1:
            return v
        return v / 100.0

    return replace(
        ctx,
        primary_kpi=primary,
        acos_constraint=norm_pct(acos_c),
        tacos_constraint=norm_pct(tacos_c),
        budget_target_from_cs=budget_target,
        season_months=season_months,
        mrr_fee=mrr,
    )


# -------------------------
# Shared evaluators
# -------------------------

def _eval_abs_delta(
    ctx: DatabricksContext,
    row: int,
    label: str,
    ok_th: float,
    partial_th: float,
    src: str,
    fmt: str = "number",
) -> cfg.ControlResult:
    """
    Reads YoY delta from 03_Yearly_KPIs_Current_vs_Last_ (B/C/D).
    NEW RULE (per your request):
      - If B or C is missing -> OK (not evaluated)
      - If B & C exist but D missing -> compute D
    Thresholds use ABS(delta) for most metrics (CPC/Impr/CTR/CVR/AOV).
    """
    df03 = get_dataset(ctx, "YEARLY_KPIS")
    why = _why_trend_metric(label)
    if df03 is None or df03.empty:
        # keeping as FLAG because this is a required tab for multiple controls
        return flag("Observed: Yearly KPI table missing; cannot evaluate YoY trend.", why, src)

    d = _read_cell_by_pos(df03, "D", row)
    b = _read_cell_by_pos(df03, "B", row)
    c = _read_cell_by_pos(df03, "C", row)

    # ✅ Only evaluate when BOTH periods exist
    if b is None or c is None:
        return ok(
            f"Observed: {label} trend not evaluated because one or both comparison periods are missing (B{row}/C{row}).",
            why,
            src,
        )

    # If D missing, compute safely
    if d is None:
        if c == 0:
            return ok(
                f"Observed: {label} trend not evaluated because previous period is zero (C{row}=0).",
                why,
                src,
            )
        d = (b - c) / c

    delta = float(d)
    ad = abs(delta)

    def fmt_val(v: float) -> str:
    if fmt == "pct":
        vv = v if v <= 1 else v / 100
        return _pct_str(vv)
    if fmt == "money2":
        return _money_str_2(v)
    return _money_str(v)

    what = f"Observed: {label} changed {_pct_str(delta)} YoY ({fmt_val(c)} → {fmt_val(b)})."

    if ad < ok_th:
        return ok(what, why, src)
    if ad <= partial_th:
        return partial(what, why, src)
    return flag(what, why, src)

def _eval_directional_delta(
    ctx: DatabricksContext,
    row: int,
    label: str,
    worse_when: str,
    src: str,
    fmt: str = "number",
    threshold: float = 0.10,
) -> cfg.ControlResult:
    """
    Reads YoY delta from 03_Yearly_KPIs_Current_vs_Last_ (B/C/D).
    Only flags when the movement is directionally worse versus last year
    AND exceeds the specified threshold.

      - worse_when = 'up'   -> higher is worse
      - worse_when = 'down' -> lower is worse
      - If B or C is missing -> OK (not evaluated)
      - If B & C exist but D missing -> compute D
    """
    df03 = get_dataset(ctx, "YEARLY_KPIS")
    why = _why_trend_metric(label)
    if df03 is None or df03.empty:
        return flag("Observed: Yearly KPI table missing; cannot evaluate YoY trend.", why, src)

    d = _read_cell_by_pos(df03, "D", row)
    b = _read_cell_by_pos(df03, "B", row)
    c = _read_cell_by_pos(df03, "C", row)

    if b is None or c is None:
        return ok(
            f"Observed: {label} trend not evaluated because one or both comparison periods are missing (B{row}/C{row}).",
            why,
            src,
        )

    if d is None:
        if c == 0:
            return ok(
                f"Observed: {label} trend not evaluated because previous period is zero (C{row}=0).",
                why,
                src,
            )
        d = (b - c) / c

    delta = float(d)

    def fmt_val(v: float) -> str:
        if fmt == "pct":
            vv = v if v <= 1 else v / 100
            return _pct_str(vv)
        return _money_str(v)

    what = f"Observed: {label} changed {_pct_str(delta)} YoY ({fmt_val(c)} → {fmt_val(b)})."

    if worse_when == "up":
        return flag(what, why, src) if delta > threshold else ok(what, why, src)

    if worse_when == "down":
        return flag(what, why, src) if delta < -threshold else ok(what, why, src)

    return ok(what, why, src)


# ---- Benchmarks ----

def _bench_compare_directional(our: float, bench: float, direction: str) -> float:
    """
    Returns a directional deviation score:
      higher_worse -> only positive deviation (our > bench)
      lower_worse  -> only negative deviation (our < bench) expressed positive
      abs          -> absolute deviation magnitude
    """
    if bench == 0:
        return float("inf")
    dev = (our - bench) / bench
    if direction == "higher_worse":
        return max(dev, 0.0)
    if direction == "lower_worse":
        return max(-dev, 0.0)
    return abs(dev)

def _bench_status(dev: float, ok_th: float, partial_th: float) -> str:
    if dev < ok_th:
        return cfg.STATUS_OK
    if dev <= partial_th:
        return cfg.STATUS_PARTIAL
    return cfg.STATUS_FLAG

def _bench_status_directional(our: float, bench: float, direction: str) -> str:
    if direction == "higher_worse":
        return cfg.STATUS_FLAG if our > bench else cfg.STATUS_OK
    if direction == "lower_worse":
        return cfg.STATUS_FLAG if our < bench else cfg.STATUS_OK
    return cfg.STATUS_OK

def _bench_missing_ok(metric_label: str, src: str, why: str) -> cfg.ControlResult:
    # ✅ Per your request: missing benchmark/account values => OK (not evaluated)
    return ok(
        f"Observed: {metric_label} benchmark comparison not evaluated because required account or benchmark data is missing.",
        why,
        src,
    )


# -------------------------
# Controls (C001–C026)
# -------------------------

def eval_C001(ctx: DatabricksContext) -> cfg.ControlResult:
    df = get_dataset(ctx, "L24M_MONTHLY")
    src = "04_L24M_Monthly_Performance_Sum!B (TotalSales)"
    last2 = _get_last_n_full_month_rows(df, ctx, n=2, month_col_index=0)
    if last2 is None or len(last2) < 2:
        return flag("Observed: Monthly performance data missing or insufficient to compute MoM revenue growth.", _why_trend_metric("revenue"), src)

    m0 = last2.iloc[-1]["_month"]
    m1 = last2.iloc[-2]["_month"]
    rev0 = _to_float(last2.iloc[-1, 1])
    rev1 = _to_float(last2.iloc[-2, 1])

    if rev0 is None or rev1 is None or rev1 == 0:
        return flag("Observed: TotalSales values missing/invalid for MoM revenue calculation.", _why_trend_metric("revenue"), src)

    mom = (rev0 - rev1) / rev1
    flag_th, partial_th = _apply_seasonality_thresholds(ctx, [pd.Timestamp(m0), pd.Timestamp(m1)], base_flag=-0.10, base_partial=-0.05)

    what = f"Observed: Revenue changed {_pct_str(mom)} vs prior month ({_money_str(rev1)} → {_money_str(rev0)}) using the latest full month available."
    why = _why_trend_metric("revenue")

    if mom <= flag_th:
        return flag(what, why, src)
    if mom <= partial_th:
        return partial(what, why, src)
    return ok(what, why, src)

def eval_C002(ctx: DatabricksContext) -> cfg.ControlResult:
    df = get_dataset(ctx, "L24M_MONTHLY")
    src = "04_L24M_Monthly_Performance_Sum!B (TotalSales)"
    last6 = _get_last_n_full_month_rows(df, ctx, n=6, month_col_index=0)
    if last6 is None or len(last6) < 6:
        return flag("Observed: Insufficient monthly history to compute QoQ revenue growth (need 6 full months).", _why_trend_metric("revenue"), src)

    vals = [_to_float(x) for x in list(last6.iloc[:, 1])]
    months = list(last6["_month"])
    if any(v is None for v in vals):
        return flag("Observed: Missing TotalSales values in 04_L24M_Monthly_Performance_Sum for QoQ computation.", _why_trend_metric("revenue"), src)

    q0 = sum(vals[-3:])
    q1 = sum(vals[:3])
    if q1 == 0:
        return flag("Observed: Previous-quarter revenue is zero/invalid; QoQ cannot be computed.", _why_trend_metric("revenue"), src)

    qoq = (q0 - q1) / q1
    flag_th, partial_th = _apply_seasonality_thresholds(ctx, [pd.Timestamp(m) for m in months], base_flag=-0.10, base_partial=-0.05)

    what = f"Observed: Revenue changed {_pct_str(qoq)} comparing the last 3 full months ({_money_str(q1)} → {_money_str(q0)})."
    why = _why_trend_metric("revenue")

    if qoq <= flag_th:
        return flag(what, why, src)
    if qoq <= partial_th:
        return partial(what, why, src)
    return ok(what, why, src)

def eval_C003(ctx: DatabricksContext) -> cfg.ControlResult:
    df = get_dataset(ctx, "YEARLY_KPIS")
    src = "03_Yearly_KPIs_Current_vs_Last_!B18/C18 (TotalSales)"
    if df is None or df.empty:
        return flag("Observed: Yearly KPI table missing; cannot evaluate YoY revenue growth.", _why_trend_metric("revenue"), src)

    cur = _read_cell_by_pos(df, "B", 18)
    prev = _read_cell_by_pos(df, "C", 18)
    if cur is None or prev is None or prev == 0:
        return flag("Observed: TotalSales values missing in 03_Yearly_KPIs_Current_vs_Last_ (B18/C18).", _why_trend_metric("revenue"), src)

    yoy = (cur - prev) / prev
    what = f"Observed: Revenue changed {_pct_str(yoy)} vs previous period ({_money_str(prev)} → {_money_str(cur)}) based on TotalSales in 03_Yearly_KPIs_Current_vs_Last_."
    why = _why_trend_metric("revenue")

    if yoy <= -0.10:
        return flag(what, why, src)
    if yoy <= -0.05:
        return partial(what, why, src)
    return ok(what, why, src)

def eval_C004(ctx: DatabricksContext) -> cfg.ControlResult:
    df02 = get_dataset(ctx, "KPI_RANGE")
    src = "02_Date_Range_KPIs__Date_Range_!M7 vs 38_Client_Success_Insights_Repo!O7"
    if df02 is None or df02.empty:
        return flag("Observed: Date range KPI tab missing; cannot evaluate ACoS goal attainment.", _why_constraint_metric(ctx, "ACOS"), src)
    if ctx.acos_constraint is None:
        return flag("Observed: ACoS constraint missing in Client Success (O7).", _why_constraint_metric(ctx, "ACOS"), src)

    actual = _read_cell_by_pos(df02, "M", 7)
    if actual is None:
        return flag("Observed: Current ACoS missing in 02_Date_Range_KPIs__Date_Range_ (M7).", _why_constraint_metric(ctx, "ACOS"), src)
    if actual > 1:
        actual = actual / 100.0

    target = ctx.acos_constraint
    what = f"Observed: Current ACoS = {_pct_str(actual)} vs ACoS constraint = {_pct_str(target)}."
    why = _why_constraint_metric(ctx, "ACOS")

    if actual <= target * 1.05:
        return ok(what, why, src)
    if actual <= target * 1.10:
        over_pp = (actual - target) * 100
        return partial(f"{what} (over by {over_pp:.1f}pp).", why, src)
    over_pp = (actual - target) * 100
    return flag(f"{what} (over by {over_pp:.1f}pp).", why, src)

def eval_C005(ctx: DatabricksContext) -> cfg.ControlResult:
    df02 = get_dataset(ctx, "KPI_RANGE")
    src = "02_Date_Range_KPIs__Date_Range_!J7 vs 38_Client_Success_Insights_Repo!AX7"
    if df02 is None or df02.empty:
        return flag("Observed: Date range KPI tab missing; cannot evaluate TACoS goal attainment.", _why_constraint_metric(ctx, "TACOS"), src)
    if ctx.tacos_constraint is None:
        return flag("Observed: TACoS constraint missing in Client Success (AX7).", _why_constraint_metric(ctx, "TACOS"), src)

    actual = _read_cell_by_pos(df02, "J", 7)
    if actual is None:
        return flag("Observed: Current TACoS missing in 02_Date_Range_KPIs__Date_Range_ (J7).", _why_constraint_metric(ctx, "TACOS"), src)
    if actual > 1:
        actual = actual / 100.0

    target = ctx.tacos_constraint
    what = f"Observed: Current TACoS = {_pct_str(actual)} vs TACoS constraint = {_pct_str(target)}."
    why = _why_constraint_metric(ctx, "TACOS")

    if actual <= target * 1.05:
        return ok(what, why, src)
    if actual <= target * 1.10:
        over_pp = (actual - target) * 100
        return partial(f"{what} (over by {over_pp:.1f}pp).", why, src)
    over_pp = (actual - target) * 100
    return flag(f"{what} (over by {over_pp:.1f}pp).", why, src)

def eval_C006(ctx: DatabricksContext) -> cfg.ControlResult:
    df02 = get_dataset(ctx, "KPI_RANGE")
    src = "02_Date_Range_KPIs__Date_Range_!G7 vs 38_Client_Success_Insights_Repo!AM7 (budget target)"
    why = f"{_primary_kpi_tag(ctx)} Budget pacing indicates whether investment levels match the planned growth/profitability strategy."
    if df02 is None or df02.empty:
        return flag("Observed: Date range KPI tab missing; cannot evaluate budget pacing.", why, src)

    spend = _read_cell_by_pos(df02, "G", 7)
    if spend is None:
        return flag("Observed: Spend value missing in 02_Date_Range_KPIs__Date_Range_ (G7).", why, src)

    if ctx.budget_target_from_cs is None:
        return ok(
            "Observed: Budget target not documented in Client Success context (AM7); budget pacing not evaluated.",
            why,
            src,
        )

    budget = float(ctx.budget_target_from_cs)
    if budget == 0:
        return flag("Observed: Budget target is zero/invalid; cannot evaluate pacing.", why, src)

    dev = (spend - budget) / budget
    abs_dev = abs(dev)

    what = f"Observed: Spend = {_money_str(spend)} vs budget target = {_money_str(budget)} (Δ {_pct_str(dev)})."

    if abs_dev <= 0.10:
        return ok(what, why, src)
    if abs_dev <= 0.20:
        return partial(what, why, src)
    return flag(what, why, src)

def eval_C007(ctx: DatabricksContext) -> cfg.ControlResult:
    return _eval_directional_delta(
        ctx=ctx,
        row=19,
        label="TACoS",
        worse_when="up",
        src="03_Yearly_KPIs_Current_vs_Last_!D19 (TACoS YoY delta)",
        fmt="pct",
        threshold=0.10,
    )

def eval_C008(ctx: DatabricksContext) -> cfg.ControlResult:
    return _eval_directional_delta(
        ctx=ctx,
        row=10,
        label="CPC",
        worse_when="up",
        src="03_Yearly_KPIs_Current_vs_Last_!D10",
        fmt="money2",
        threshold=0.10,
    )

def eval_C009(ctx: DatabricksContext) -> cfg.ControlResult:
    return _eval_directional_delta(
        ctx=ctx,
        row=7,
        label="Impressions",
        worse_when="down",
        src="03_Yearly_KPIs_Current_vs_Last_!D7",
        fmt="number",
        threshold=0.10,
    )

def eval_C010(ctx: DatabricksContext) -> cfg.ControlResult:
    return _eval_directional_delta(
        ctx=ctx,
        row=8,
        label="CTR",
        worse_when="down",
        src="03_Yearly_KPIs_Current_vs_Last_!D8",
        fmt="pct",
        threshold=0.10,
    )

def eval_C011(ctx: DatabricksContext) -> cfg.ControlResult:
    return _eval_directional_delta(
        ctx=ctx,
        row=12,
        label="Conversion Rate",
        worse_when="down",
        src="03_Yearly_KPIs_Current_vs_Last_!D12",
        fmt="pct",
        threshold=0.10,
    )

def eval_C012(ctx: DatabricksContext) -> cfg.ControlResult:
    return _eval_directional_delta(
        ctx=ctx,
        row=14,
        label="AOV",
        worse_when="down",
        src="03_Yearly_KPIs_Current_vs_Last_!D14",
        fmt="number",
        threshold=0.10,
    )
    
def eval_C013(ctx: DatabricksContext) -> cfg.ControlResult:
    src = "38_Client_Success_Insights_Repo!AG7 ÷ avg(05_Monthly_Sales_YoY_Comparison!C last 3 full months)"
    why = "Fees-to-sales ratio indicates whether service costs remain sustainable relative to account revenue size."
    if ctx.mrr_fee is None:
        return flag("Observed: MRR fee missing in Client Success repo (AG7).", why, src)

    df05 = get_dataset(ctx, "MONTHLY_YOY")
    last3 = _get_last_n_full_month_rows(df05, ctx, n=3, month_col_index=0)
    if last3 is None or len(last3) < 3:
        return flag("Observed: Monthly sales YoY tab missing/insufficient to compute last-3-month average sales for fee ratio.", why, src)

    sales_vals = [_to_float(x) for x in list(last3.iloc[:, 2])]
    if any(v is None for v in sales_vals):
        return flag("Observed: Total sales values missing in 05_Monthly_Sales_YoY_Comparison column C for fee ratio computation.", why, src)

    avg_sales = sum(sales_vals) / 3.0
    if avg_sales == 0:
        return flag("Observed: 3-month average sales is zero/invalid; cannot compute fee ratio.", why, src)

    ratio = float(ctx.mrr_fee) / avg_sales
    what = f"Observed: Monthly fee = {_money_str(float(ctx.mrr_fee))} and the 3-month average revenue = {_money_str(avg_sales)}, resulting in a fee-to-sales ratio of {_pct_str(ratio)}."

    if ratio < 0.10:
        return ok(what, why, src)
    if ratio <= 0.15:
        return partial(what, why, src)
    return flag(what, why, src)

def eval_C014(ctx: DatabricksContext) -> cfg.ControlResult:
    return ok(
        "Observed: NTB% dataset not available in the Databricks export; control currently not evaluated.",
        "NTB% dataset pending integration (AMC source)",
        "AMC (external)",
    )

def eval_C015(ctx: DatabricksContext) -> cfg.ControlResult:
    return ok(
        "Observed: Organic rank dataset not available in the Databricks export; control currently not evaluated.",
        "Organic rank dataset pending integration",
        "External dataset (pending)",
    )

def eval_C016(ctx: DatabricksContext) -> cfg.ControlResult:
    df42 = get_dataset(ctx, "GGS_DOMO")
    src = "42_Amazon_GGS_Domo!H7/H8/H9"
    why = "GGS pacing flags indicate whether planned growth programs are executing as expected."
    if df42 is None or df42.empty:
        return flag("Observed: GGS Domo tab missing; cannot evaluate GGS pacing.", why, src)
    h7 = _read_str_cell_by_pos(df42, "H", 7).lower()
    h8 = _read_str_cell_by_pos(df42, "H", 8).lower()
    h9 = _read_str_cell_by_pos(df42, "H", 9).lower()
    if not (h7 or h8 or h9):
        return flag("Observed: GGS pacing fields missing in 42_Amazon_GGS_Domo (H7–H9).", why, src)
    what = f"Observed: GGS pacing flags (SD/SP/SB) = {h7}/{h8}/{h9}."
    if any(v == "yes" for v in [h7, h8, h9]):
        return flag(what, why, src)
    return ok(what, why, src)

def eval_C017(ctx: DatabricksContext) -> cfg.ControlResult:
    df42 = get_dataset(ctx, "GGS_DOMO")
    src = "42_Amazon_GGS_Domo!K7/K8/K9"
    why = "DAA pacing flags indicate whether key demand acceleration investments are on track."
    if df42 is None or df42.empty:
        return flag("Observed: GGS Domo tab missing; cannot evaluate DAA pacing.", why, src)
    k7 = _read_str_cell_by_pos(df42, "K", 7).lower()
    k8 = _read_str_cell_by_pos(df42, "K", 8).lower()
    k9 = _read_str_cell_by_pos(df42, "K", 9).lower()
    if not (k7 or k8 or k9):
        return flag("Observed: DAA pacing fields missing in 42_Amazon_GGS_Domo (K7–K9).", why, src)
    what = f"Observed: DAA pacing flags (SD/SP/SB) = {k7}/{k8}/{k9}."
    if any(v == "true" for v in [k7, k8, k9]):
        return flag(what, why, src)
    return ok(what, why, src)

def eval_C018(ctx: DatabricksContext) -> cfg.ControlResult:
    df42 = get_dataset(ctx, "GGS_DOMO")
    src = "42_Amazon_GGS_Domo!M7/M8/M9"
    why = "SAS pacing flags indicate whether strategic support initiatives are executing as committed."
    if df42 is None or df42.empty:
        return flag("Observed: GGS Domo tab missing; cannot evaluate SAS pacing.", why, src)
    m7 = _read_str_cell_by_pos(df42, "M", 7).lower()
    m8 = _read_str_cell_by_pos(df42, "M", 8).lower()
    m9 = _read_str_cell_by_pos(df42, "M", 9).lower()
    if not (m7 or m8 or m9):
        return flag("Observed: SAS pacing fields missing in 42_Amazon_GGS_Domo (M7–M9).", why, src)
    what = f"Observed: SAS pacing flags (SD/SP/SB) = {m7}/{m8}/{m9}."
    if any(v == "true" for v in [m7, m8, m9]):
        return flag(what, why, src)
    return ok(what, why, src)

def eval_C019(ctx: DatabricksContext) -> cfg.ControlResult:
    df46 = get_dataset(ctx, "STRIPE")
    src = "46_Stripe_Payments!C (PaymentDate)"
    why = "Delayed or missed payments increase client risk exposure and indicate potential retention or financial stress."

    if df46 is None or df46.empty:
        return flag("Observed: Stripe payments tab missing; cannot evaluate Financial Risk Indicator.", why, src)

    try:
        pay_dates = pd.to_datetime(df46.iloc[:, 2], errors="coerce").dropna()
    except Exception:
        pay_dates = pd.Series([], dtype="datetime64[ns]")

    if pay_dates.empty:
        return flag(
            "Observed: No valid payment dates found in 46_Stripe_Payments column C; cannot evaluate payment punctuality.",
            why,
            src,
        )

    first_payment = pay_dates.min().date()
    billing_day = first_payment.day

    anchor = (
        pd.Timestamp(ctx.ref_date).to_period("M").to_timestamp()
        if ctx.ref_date
        else pay_dates.max().to_period("M").to_timestamp()
    )

    months = [(anchor - pd.offsets.MonthBegin(i)).to_period("M").to_timestamp() for i in range(1, 7)]

    pay_df = pd.DataFrame({"dt": pay_dates})
    pay_df["y"] = pay_df["dt"].dt.year
    pay_df["m"] = pay_df["dt"].dt.month
    pay_df["d"] = pay_df["dt"].dt.day
    days_by_month = pay_df.groupby(["y", "m"])["d"].apply(set).to_dict()

    missing_last3 = []
    missing_3to6 = []

    def _dim(y: int, m: int) -> int:
        return pd.Period(f"{y}-{m:02d}", freq="M").days_in_month

    for idx, m in enumerate(months, start=1):
        day = min(billing_day, _dim(m.year, m.month))
        lower = max(1, day - 3)
        upper = min(_dim(m.year, m.month), day + 3)

        paid_days = days_by_month.get((m.year, m.month), set())
        on_time = any(lower <= d <= upper for d in paid_days)

        if not on_time:
            expected_window = f"{m.year}-{m.month:02d}-{lower:02d} to {m.year}-{m.month:02d}-{upper:02d}"
            if idx <= 3:
                missing_last3.append(expected_window)
            else:
                missing_3to6.append(expected_window)

    what = (
        f"Observed: Billing day is day {billing_day} (first payment: {first_payment}). "
        f"Expected payment window is billing_day ± 3 days. "
        f"Missing payments detected in: {missing_last3} (last 3 months), {missing_3to6} (months 3–6)."
    )

    if missing_last3:
        return flag(what, why, src)
    if missing_3to6:
        return partial(what, why, src)
    return ok(what, why, src)

def eval_C020(ctx: DatabricksContext) -> cfg.ControlResult:
    return ok(
        "Observed: Churn risk dataset not available in the Databricks export; control currently not evaluated.",
        "Churn Zero Score dataset pending integration",
        "External dataset (pending)",
    )

def eval_C021(ctx: DatabricksContext) -> cfg.ControlResult:
    df = get_dataset(ctx, "COHORT_BENCH")
    src = "43_Cohort_Main_Category_Perform!B7 vs J7"
    why = _why_benchmark("ACoS", "higher_worse")

    if df is None or df.empty:
        return _bench_missing_ok("ACoS", src, why)

    our = _read_cell_by_pos(df, "B", 7)
    bench = _read_cell_by_pos(df, "J", 7)
    if our is None or bench is None or bench == 0:
        return _bench_missing_ok("ACoS", src, why)

    if our > 1:
        our /= 100
    if bench > 1:
        bench /= 100

    status = _bench_status_directional(our, bench, "higher_worse")
    what = f"Observed: ACoS = {_pct_str(our)} vs category benchmark = {_pct_str(bench)}."
    return cfg.ControlResult(status=status, what_we_saw=what, why_it_matters=why, data_source=src)

def eval_C022(ctx: DatabricksContext) -> cfg.ControlResult:
    df = get_dataset(ctx, "COHORT_BENCH")
    src = "43_Cohort_Main_Category_Perform!D7 vs N7"
    why = _why_benchmark("Conversion Rate", "lower_worse")

    if df is None or df.empty:
        return _bench_missing_ok("Conversion Rate", src, why)

    our = _read_cell_by_pos(df, "D", 7)
    bench = _read_cell_by_pos(df, "N", 7)
    if our is None or bench is None or bench == 0:
        return _bench_missing_ok("Conversion Rate", src, why)

    if our > 1:
        our /= 100
    if bench > 1:
        bench /= 100

    status = _bench_status_directional(our, bench, "lower_worse")
    what = f"Observed: Conversion Rate = {_pct_str(our)} vs category benchmark = {_pct_str(bench)}."
    return cfg.ControlResult(status=status, what_we_saw=what, why_it_matters=why, data_source=src)

def eval_C023(ctx: DatabricksContext) -> cfg.ControlResult:
    df = get_dataset(ctx, "COHORT_BENCH")
    src = "43_Cohort_Main_Category_Perform!C7 vs L7"
    why = _why_benchmark("TACoS", "higher_worse")

    if df is None or df.empty:
        return _bench_missing_ok("TACoS", src, why)

    our = _read_cell_by_pos(df, "C", 7)
    bench = _read_cell_by_pos(df, "L", 7)
    if our is None or bench is None or bench == 0:
        return _bench_missing_ok("TACoS", src, why)

    if our > 1:
        our /= 100
    if bench > 1:
        bench /= 100

    status = _bench_status_directional(our, bench, "higher_worse")
    what = f"Observed: TACoS = {_pct_str(our)} vs category benchmark = {_pct_str(bench)}."
    return cfg.ControlResult(status=status, what_we_saw=what, why_it_matters=why, data_source=src)

def eval_C024(ctx: DatabricksContext) -> cfg.ControlResult:
    df = get_dataset(ctx, "COHORT_BENCH")
    src = "43_Cohort_Main_Category_Perform!E7 vs P7"
    why = _why_benchmark("CPC", "higher_worse")

    if df is None or df.empty:
        return _bench_missing_ok("CPC", src, why)

    our = _read_cell_by_pos(df, "E", 7)
    bench = _read_cell_by_pos(df, "P", 7)
    if our is None or bench is None or bench == 0:
        return _bench_missing_ok("CPC", src, why)

    status = _bench_status_directional(our, bench, "higher_worse")
    what = f"Observed: CPC = {_money_str_2(our)} vs category benchmark = {_money_str_2(bench)}."
    return cfg.ControlResult(status=status, what_we_saw=what, why_it_matters=why, data_source=src)

def eval_C025(ctx: DatabricksContext) -> cfg.ControlResult:
    df = get_dataset(ctx, "COHORT_BENCH")
    src = "43_Cohort_Main_Category_Perform!F7 vs R7"
    why = _why_benchmark("Organic Sales Rate", "lower_worse")

    if df is None or df.empty:
        return _bench_missing_ok("Organic Sales Rate", src, why)

    our = _read_cell_by_pos(df, "F", 7)
    bench = _read_cell_by_pos(df, "R", 7)
    if our is None or bench is None or bench == 0:
        return _bench_missing_ok("Organic Sales Rate", src, why)

    if our > 1:
        our /= 100
    if bench > 1:
        bench /= 100

    status = _bench_status_directional(our, bench, "lower_worse")
    what = f"Observed: Organic Sales Rate = {_pct_str(our)} vs category benchmark = {_pct_str(bench)}."
    return cfg.ControlResult(status=status, what_we_saw=what, why_it_matters=why, data_source=src)

def eval_C026(ctx: DatabricksContext) -> cfg.ControlResult:
    df = get_dataset(ctx, "COHORT_BENCH")
    src = "43_Cohort_Main_Category_Perform!G7 vs T7"
    why = _why_benchmark("Sales Growth", "lower_worse")

    if df is None or df.empty:
        return _bench_missing_ok("Sales Growth", src, why)

    our = _read_cell_by_pos(df, "G", 7)
    bench = _read_cell_by_pos(df, "T", 7)
    if our is None or bench is None or bench == 0:
        return _bench_missing_ok("Sales Growth", src, why)

    if our > 1:
        our /= 100
    if bench > 1:
        bench /= 100

    status = _bench_status_directional(our, bench, "lower_worse")
    what = f"Observed: Sales Growth = {_pct_str(our)} vs category benchmark = {_pct_str(bench)}."
    return cfg.ControlResult(status=status, what_we_saw=what, why_it_matters=why, data_source=src)


def evaluate_all(ctx: DatabricksContext) -> Tuple[Dict[str, cfg.ControlResult], DatabricksContext]:
    ctx = hydrate_constraints_and_context(ctx)

    fns = {
        "C001": eval_C001,
        "C002": eval_C002,
        "C003": eval_C003,
        "C004": eval_C004,
        "C005": eval_C005,
        "C006": eval_C006,
        "C007": eval_C007,
        "C008": eval_C008,
        "C009": eval_C009,
        "C010": eval_C010,
        "C011": eval_C011,
        "C012": eval_C012,
        "C013": eval_C013,
        "C014": eval_C014,
        "C015": eval_C015,
        "C016": eval_C016,
        "C017": eval_C017,
        "C018": eval_C018,
        "C019": eval_C019,
        "C020": eval_C020,
        "C021": eval_C021,
        "C022": eval_C022,
        "C023": eval_C023,
        "C024": eval_C024,
        "C025": eval_C025,
        "C026": eval_C026,
    }

    results: Dict[str, cfg.ControlResult] = {}
    for cid, fn in fns.items():
        try:
            results[cid] = fn(ctx)
        except Exception as e:
            results[cid] = cfg.ControlResult(
                status=cfg.STATUS_FLAG,
                what_we_saw=f"Observed: EXCEPTION in {cid}: {e}",
                why_it_matters="A processing error prevented evaluation; requires investigation to ensure diagnostic accuracy.",
                data_source="Agent Runtime",
                note=str(e),
            )

    return results, ctx
