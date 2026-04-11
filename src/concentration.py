from __future__ import annotations

import pandas as pd
import numpy as np


CONCENTRATION_LIMITS = {
    "single_borrower_ead_pct": 0.10,
    "industry_ead_pct": 0.25,
    "region_ead_pct": 0.30,
    "top_n_borrowers": 10,
    "hhi_warning_threshold": 0.15,
}


def single_name_concentration(
    df: pd.DataFrame,
    limit_pct: float | None = None,
    top_n: int | None = None,
) -> pd.DataFrame:
    """Analyse single-name (borrower) concentration risk.

    Returns one row per borrower sorted by EAD descending, with portfolio
    share and breach flag against the single-name limit.
    """
    limit = limit_pct or CONCENTRATION_LIMITS["single_borrower_ead_pct"]
    n = top_n or CONCENTRATION_LIMITS["top_n_borrowers"]
    total_ead = df["ead"].sum()

    borrower_agg = df.groupby("borrower_id", sort=False).agg(
        facility_count=("facility_id", "count"),
        total_ead=("ead", "sum"),
        total_el=("expected_loss", "sum"),
    ).reset_index()

    borrower_agg["portfolio_share"] = (borrower_agg["total_ead"] / total_ead).round(6)
    borrower_agg["breach_flag"] = borrower_agg["portfolio_share"] > limit
    borrower_agg = borrower_agg.sort_values("total_ead", ascending=False).reset_index(drop=True)
    borrower_agg["cumulative_share"] = borrower_agg["portfolio_share"].cumsum().round(6)
    borrower_agg["rank"] = range(1, len(borrower_agg) + 1)

    return borrower_agg.head(n)


def sector_concentration(
    df: pd.DataFrame,
    limit_pct: float | None = None,
) -> pd.DataFrame:
    """Analyse industry sector concentration risk.

    Returns one row per industry with EAD share, EL contribution, and
    Herfindahl-Hirschman Index contribution.
    """
    limit = limit_pct or CONCENTRATION_LIMITS["industry_ead_pct"]
    total_ead = df["ead"].sum()
    total_el = df["expected_loss"].sum()

    pd_col = "pd_final" if "pd_final" in df.columns else "pd"
    lgd_col = "lgd_final" if "lgd_final" in df.columns else "lgd"

    sector_agg = df.groupby("industry", sort=False).agg(
        facility_count=("facility_id", "count"),
        borrower_count=("borrower_id", "nunique"),
        total_ead=("ead", "sum"),
        total_el=("expected_loss", "sum"),
    ).reset_index()

    sector_agg["ead_share"] = (sector_agg["total_ead"] / total_ead).round(6)
    sector_agg["el_share"] = (sector_agg["total_el"] / total_el).round(6) if total_el else 0.0
    sector_agg["hhi_contribution"] = (sector_agg["ead_share"] ** 2).round(6)
    sector_agg["breach_flag"] = sector_agg["ead_share"] > limit
    sector_agg = sector_agg.sort_values("total_ead", ascending=False).reset_index(drop=True)

    return sector_agg


def region_concentration(
    df: pd.DataFrame,
    limit_pct: float | None = None,
) -> pd.DataFrame:
    """Analyse geographic (region) concentration risk."""
    limit = limit_pct or CONCENTRATION_LIMITS["region_ead_pct"]
    total_ead = df["ead"].sum()
    total_el = df["expected_loss"].sum()

    region_agg = df.groupby("region", sort=False).agg(
        facility_count=("facility_id", "count"),
        borrower_count=("borrower_id", "nunique"),
        total_ead=("ead", "sum"),
        total_el=("expected_loss", "sum"),
    ).reset_index()

    region_agg["ead_share"] = (region_agg["total_ead"] / total_ead).round(6)
    region_agg["el_share"] = (region_agg["total_el"] / total_el).round(6) if total_el else 0.0
    region_agg["hhi_contribution"] = (region_agg["ead_share"] ** 2).round(6)
    region_agg["breach_flag"] = region_agg["ead_share"] > limit
    region_agg = region_agg.sort_values("total_ead", ascending=False).reset_index(drop=True)

    return region_agg


def portfolio_hhi(df: pd.DataFrame, dimension: str = "industry") -> dict:
    """Compute Herfindahl-Hirschman Index for a given dimension.

    HHI ranges from 0 (perfectly diversified) to 1 (fully concentrated).
    Values above the warning threshold indicate elevated concentration.
    """
    total_ead = df["ead"].sum()
    if total_ead == 0:
        return {"dimension": dimension, "hhi": 0.0, "status": "no_exposure"}

    shares = df.groupby(dimension)["ead"].sum() / total_ead
    hhi = float((shares ** 2).sum())
    threshold = CONCENTRATION_LIMITS["hhi_warning_threshold"]

    return {
        "dimension": dimension,
        "hhi": round(hhi, 6),
        "effective_number": round(1 / hhi, 1) if hhi > 0 else 0.0,
        "status": "elevated" if hhi > threshold else "within_appetite",
        "threshold": threshold,
    }


def concentration_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Produce a combined concentration risk summary across all dimensions."""
    records = []

    for dim in ["industry", "region", "borrower_id"]:
        if dim not in df.columns:
            continue
        hhi_result = portfolio_hhi(df, dim)
        total_ead = df["ead"].sum()
        top_share = df.groupby(dim)["ead"].sum().max() / total_ead if total_ead else 0.0
        segment_count = df[dim].nunique()

        records.append({
            "dimension": dim,
            "segment_count": segment_count,
            "hhi": hhi_result["hhi"],
            "effective_number": hhi_result["effective_number"],
            "top_segment_share": round(float(top_share), 4),
            "status": hhi_result["status"],
        })

    return pd.DataFrame.from_records(records)
