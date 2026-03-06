# config_health.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

# ---------- Status (locked) ----------
STATUS_OK = "OK"
STATUS_FLAG = "FLAG"
STATUS_PARTIAL = "PARTIAL"


# ---------- ControlResult (locked contract) ----------
@dataclass(frozen=True)
class ControlResult:
    status: str
    what_we_saw: str = ""
    why_it_matters: str = ""   # NEW
    data_source: str = ""
    note: str = ""  # optional internal note


# ---------- Databricks workbook sheet candidates ----------
TAB_CANDIDATES: Dict[str, List[str]] = {
    # SSOT header
    "HEADER": ["01_Advertiser_Name"],

    # KPI / Performance
    "KPI_RANGE": ["02_Date_Range_KPIs__Date_Range_"],
    "YEARLY_KPIS": ["03_Yearly_KPIs_Current_vs_Last_"],
    "L24M_MONTHLY": ["04_L24M_Monthly_Performance_Sum"],
    "MONTHLY_YOY": ["05_Monthly_Sales_YoY_Comparison"],

    # Client Success / Constraints / Fees
    "CS_REPO": ["38_Client_Success_Insights_Repo"],

    # GGS & commitment
    "GGS_DOMO": ["42_Amazon_GGS_Domo"],

    # Benchmarks
    "COHORT_BENCH": ["43_Cohort_Main_Category_Perform"],

    # Payments / Financial risk
    "STRIPE": ["46_Stripe_Payments"],
}
