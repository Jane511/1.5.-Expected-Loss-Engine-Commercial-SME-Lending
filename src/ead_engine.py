from __future__ import annotations

import numpy as np
import pandas as pd

from .config import CCF_LOOKUP


def _risk_grade_series(df: pd.DataFrame) -> pd.Series:
    if "risk_grade" in df.columns:
        return df["risk_grade"].astype(str)
    return df["internal_risk_grade"].astype(str)


def _is_revolving(df: pd.DataFrame) -> pd.Series:
    if "loan_type" in df.columns:
        return df["loan_type"].astype(str).str.lower().eq("revolving")
    return df["product_type"].astype(str).str.contains("Overdraft", case=False, na=False)


def assign_ccf_bucket(df: pd.DataFrame) -> pd.Series:
    risk_grade = _risk_grade_series(df)
    watchlist = pd.to_numeric(
        df.get("watchlist_flag", pd.Series(0, index=df.index)),
        errors="coerce",
    ).fillna(0).astype(int)
    arrears = pd.to_numeric(
        df.get("arrears_days", pd.Series(0, index=df.index)),
        errors="coerce",
    ).fillna(0)
    revolving = _is_revolving(df)

    weak = revolving & ((watchlist == 1) | (arrears >= 30) | risk_grade.isin(["RG4", "RG5"]))
    average = revolving & ~weak & risk_grade.eq("RG3")

    bucket = pd.Series("not_applicable", index=df.index, dtype="object")
    bucket.loc[revolving] = "strong"
    bucket.loc[average] = "average"
    bucket.loc[weak] = "weak"
    return bucket


def add_ead_columns(df: pd.DataFrame, ccf_multiplier: float = 1.0) -> pd.DataFrame:
    out = df.copy()
    revolving = _is_revolving(out)
    if "undrawn_amount" in out.columns:
        out["undrawn_amount"] = out["undrawn_amount"].fillna(
            (out["limit_amount"] - out["drawn_balance"]).clip(lower=0.0)
        )
    else:
        out["undrawn_amount"] = (out["limit_amount"] - out["drawn_balance"]).clip(lower=0.0)

    derived_bucket = assign_ccf_bucket(out)
    if "ccf_bucket" in out.columns:
        out["ccf_bucket"] = out["ccf_bucket"].fillna(derived_bucket)
    else:
        out["ccf_bucket"] = derived_bucket

    if "ccf_base" in out.columns:
        out["ccf_base"] = pd.to_numeric(out["ccf_base"], errors="coerce").fillna(
            out["ccf_bucket"].map(CCF_LOOKUP).fillna(0.0)
        )
    else:
        out["ccf_base"] = out["ccf_bucket"].map(CCF_LOOKUP).fillna(0.0)

    out["ccf_applied"] = np.where(revolving, np.minimum(out["ccf_base"] * ccf_multiplier, 1.0), 0.0)
    out["ead"] = np.where(
        revolving,
        out["drawn_balance"] + out["undrawn_amount"] * out["ccf_applied"],
        out["drawn_balance"],
    )
    out["ead"] = out["ead"].round(2)
    return out
