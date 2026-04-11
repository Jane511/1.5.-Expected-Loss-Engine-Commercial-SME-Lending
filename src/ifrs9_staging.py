from __future__ import annotations

import pandas as pd
import numpy as np


STAGE_CRITERIA = {
    "pd_sicr_threshold": 0.04,
    "pd_relative_increase": 2.0,
    "arrears_stage2_days": 30,
    "arrears_stage3_days": 90,
    "watchlist_triggers_stage2": True,
    "default_risk_grades": ("RG5",),
    "stage2_risk_grades": ("RG4",),
}


def allocate_stage(
    df: pd.DataFrame,
    criteria: dict | None = None,
) -> pd.DataFrame:
    """Allocate IFRS 9 impairment stage (1, 2, or 3) to each facility.

    Stage 3 – credit-impaired (default): arrears >= 90 days OR risk grade
    in default bucket.
    Stage 2 – significant increase in credit risk (SICR): arrears >= 30 days,
    watchlist flag, PD above absolute threshold, PD relative increase > 2x,
    or risk grade in stage-2 bucket.
    Stage 1 – performing: everything else (12-month ECL horizon).
    """
    c = {**STAGE_CRITERIA, **(criteria or {})}
    out = df.copy()

    pd_col = "pd_final" if "pd_final" in out.columns else "pd"
    rg_col = "risk_grade" if "risk_grade" in out.columns else "internal_risk_grade"
    arrears_col = "arrears_days" if "arrears_days" in out.columns else None

    stage = pd.Series(1, index=out.index, dtype=int)

    if arrears_col and arrears_col in out.columns:
        arrears_values = pd.to_numeric(out[arrears_col], errors="coerce").fillna(-1)
        stage = stage.where(arrears_values < c["arrears_stage2_days"], 2)
        stage = stage.where(arrears_values < c["arrears_stage3_days"], 3)

    if rg_col in out.columns:
        stage = np.where(out[rg_col].isin(c["stage2_risk_grades"]) & (stage < 2), 2, stage)
        stage = np.where(out[rg_col].isin(c["default_risk_grades"]) & (stage < 3), 3, stage)

    if "watchlist_flag" in out.columns and c["watchlist_triggers_stage2"]:
        is_watchlist = out["watchlist_flag"].astype(str).str.lower().isin(["true", "1", "yes", "y"])
        stage = np.where(is_watchlist & (stage < 2), 2, stage)

    if pd_col in out.columns:
        stage = np.where((out[pd_col] >= c["pd_sicr_threshold"]) & (stage < 2), 2, stage)

    out["ifrs9_stage"] = pd.Series(stage, index=out.index).astype(int)
    return out


def compute_ecl(df: pd.DataFrame) -> pd.DataFrame:
    """Compute 12-month and lifetime ECL based on IFRS 9 stage allocation.

    Stage 1: ECL = PD_12m * LGD * EAD
    Stage 2: ECL = lifetime_PD * LGD * EAD  (simplified: PD * maturity scalar)
    Stage 3: ECL = LGD * EAD  (default assumed; PD effectively = 1)
    """
    out = df.copy()

    pd_col = "pd_final" if "pd_final" in out.columns else "pd"
    lgd_col = "lgd_final" if "lgd_final" in out.columns else "lgd"

    if "loan_term_months" in out.columns:
        maturity_years = (out["loan_term_months"] / 12).clip(lower=1)
    elif "maturity" in out.columns:
        maturity_years = out["maturity"].clip(lower=1)
    else:
        maturity_years = pd.Series(3.0, index=out.index)

    pd_12m = out[pd_col].clip(0, 1)
    lifetime_pd = (pd_12m * maturity_years * 1.15).clip(upper=0.999)

    ecl_stage1 = pd_12m * out[lgd_col] * out["ead"]
    ecl_stage2 = lifetime_pd * out[lgd_col] * out["ead"]
    ecl_stage3 = out[lgd_col] * out["ead"]

    out["ecl_12m"] = ecl_stage1.round(2)
    out["ecl_lifetime"] = ecl_stage2.round(2)

    out["ecl_provision"] = np.select(
        [out["ifrs9_stage"] == 1, out["ifrs9_stage"] == 2, out["ifrs9_stage"] == 3],
        [ecl_stage1, ecl_stage2, ecl_stage3],
        default=ecl_stage1,
    ).round(2)

    out["ecl_coverage_ratio"] = (
        out["ecl_provision"] / out["ead"].replace(0, pd.NA)
    ).fillna(0).round(6)

    return out


def summarise_by_stage(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate ECL provisions and portfolio metrics by IFRS 9 stage."""
    pd_col = "pd_final" if "pd_final" in df.columns else "pd"
    lgd_col = "lgd_final" if "lgd_final" in df.columns else "lgd"

    records = []
    for stage, grp in df.groupby("ifrs9_stage", sort=True):
        total_ead = grp["ead"].sum()
        total_ecl = grp["ecl_provision"].sum()
        records.append({
            "ifrs9_stage": int(stage),
            "stage_label": {1: "Performing", 2: "Under-performing (SICR)", 3: "Credit-impaired"}[stage],
            "facility_count": len(grp),
            "total_ead": round(float(total_ead), 2),
            "total_ecl_provision": round(float(total_ecl), 2),
            "ecl_coverage_ratio": round(float(total_ecl / total_ead), 6) if total_ead else 0.0,
            "avg_pd": round(float((grp[pd_col] * grp["ead"]).sum() / total_ead), 6) if total_ead else 0.0,
            "avg_lgd": round(float((grp[lgd_col] * grp["ead"]).sum() / total_ead), 6) if total_ead else 0.0,
            "share_of_portfolio_ead": round(float(total_ead / df["ead"].sum()), 4) if df["ead"].sum() else 0.0,
            "share_of_portfolio_ecl": round(float(total_ecl / df["ecl_provision"].sum()), 4) if df["ecl_provision"].sum() else 0.0,
        })

    total_ead = df["ead"].sum()
    total_ecl = df["ecl_provision"].sum()
    records.append({
        "ifrs9_stage": 0,
        "stage_label": "Total Portfolio",
        "facility_count": len(df),
        "total_ead": round(float(total_ead), 2),
        "total_ecl_provision": round(float(total_ecl), 2),
        "ecl_coverage_ratio": round(float(total_ecl / total_ead), 6) if total_ead else 0.0,
        "avg_pd": round(float((df[pd_col] * df["ead"]).sum() / total_ead), 6) if total_ead else 0.0,
        "avg_lgd": round(float((df[lgd_col] * df["ead"]).sum() / total_ead), 6) if total_ead else 0.0,
        "share_of_portfolio_ead": 1.0,
        "share_of_portfolio_ecl": 1.0,
    })
    return pd.DataFrame.from_records(records)


def stage_migration_matrix(df_prior: pd.DataFrame, df_current: pd.DataFrame) -> pd.DataFrame:
    """Build a stage migration matrix between two reporting dates.

    Both DataFrames must contain facility_id and ifrs9_stage columns.
    Returns a pivot table showing count and EAD flows between stages.
    """
    merged = df_prior[["facility_id", "ifrs9_stage"]].merge(
        df_current[["facility_id", "ifrs9_stage", "ead"]],
        on="facility_id",
        suffixes=("_prior", "_current"),
    )
    pivot = merged.groupby(["ifrs9_stage_prior", "ifrs9_stage_current"], sort=True).agg(
        facility_count=("facility_id", "count"),
        total_ead=("ead", "sum"),
    ).reset_index()
    pivot["total_ead"] = pivot["total_ead"].round(2)
    return pivot
