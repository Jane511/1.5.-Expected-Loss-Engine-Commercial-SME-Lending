from __future__ import annotations

import numpy as np
import pandas as pd

from .ead_engine import add_ead_columns


PD_COLUMNS = [
    "facility_id",
    "pd_model_stream",
    "score",
    "score_band",
    "risk_grade",
    "pd_final",
    "default_horizon_months",
    "pd_model_name",
    "pd_model_version",
    "as_of_date",
]

LGD_COLUMNS = [
    "facility_id",
    "conduct_classification",
    "lgd_base",
    "lgd_adj_lvr",
    "lgd_adj_stage",
    "lgd_adj_industry",
    "lgd_adj_pd_band",
    "lgd_adj_dscr",
    "lgd_adj_conduct",
    "lgd_adjusted",
    "downturn_scalar",
    "lgd_downturn",
    "lgd_final",
]


def _prepare_lgd_inputs(lgd_df: pd.DataFrame) -> pd.DataFrame:
    out = lgd_df.copy()
    if "facility_id" not in out.columns and "loan_id" in out.columns:
        out = out.rename(columns={"loan_id": "facility_id"})
    if "facility_id" not in out.columns:
        raise ValueError("LGD input must include facility_id")
    if "lgd_final" not in out.columns:
        raise ValueError("LGD input must include lgd_final")
    if "downturn_lgd" not in out.columns:
        out["downturn_lgd"] = out["lgd_final"]
    if "downturn_scalar" not in out.columns:
        denominator = out["lgd_final"].replace(0, pd.NA)
        out["downturn_scalar"] = (out["downturn_lgd"] / denominator).fillna(1.0)
    for column in LGD_COLUMNS:
        if column not in out.columns:
            out[column] = pd.NA
    return out[LGD_COLUMNS]


def _prepare_pd_inputs(pd_df: pd.DataFrame) -> pd.DataFrame:
    out = pd_df.copy()
    rename_map = {
        "pd_estimate": "pd_final",
    }
    out = out.rename(columns={key: value for key, value in rename_map.items() if key in out.columns})
    if "facility_id" not in out.columns:
        raise ValueError("PD input must include facility_id")
    if "pd_final" not in out.columns:
        raise ValueError("PD input must include pd_final")

    if "pd_model_stream" not in out.columns:
        if "product_type" in out.columns:
            out["pd_model_stream"] = np.where(
                out["product_type"].astype(str).str.contains("mortgage|property", case=False, na=False),
                "property",
                "cashflow",
            )
        else:
            out["pd_model_stream"] = pd.NA
    for column in PD_COLUMNS:
        if column not in out.columns:
            if column == "default_horizon_months":
                out[column] = 12
            else:
                out[column] = pd.NA
    return out[PD_COLUMNS]


def _prepare_ead_inputs(ead_df: pd.DataFrame) -> pd.DataFrame:
    out = ead_df.copy()
    if "facility_id" not in out.columns:
        raise ValueError("EAD input must include facility_id")

    rename_map = {
        "limit": "limit_amount_external",
        "limit_amount": "limit_amount_external",
        "drawn": "drawn_balance_external",
        "drawn_balance": "drawn_balance_external",
        "undrawn": "undrawn_amount_external",
        "undrawn_amount": "undrawn_amount_external",
        "utilisation": "utilisation_external",
        "base_ccf": "ccf_base_external",
        "ccf_base": "ccf_base_external",
        "downturn_ccf": "ccf_downturn_external",
        "ccf_downturn": "ccf_downturn_external",
        "ead_central": "ead_external",
        "ead": "ead_external",
        "ead_downturn": "ead_downturn_external",
    }
    out = out.rename(columns={key: value for key, value in rename_map.items() if key in out.columns})

    selected = ["facility_id"]
    selected.extend(
        column
        for column in [
            "limit_amount_external",
            "drawn_balance_external",
            "undrawn_amount_external",
            "utilisation_external",
            "ccf_base_external",
            "ccf_downturn_external",
            "ead_external",
            "ead_downturn_external",
        ]
        if column in out.columns
    )
    return out[selected]


def _prepare_industry_inputs(
    industry_scores_df: pd.DataFrame | None,
    industry_downturn_df: pd.DataFrame | None,
) -> pd.DataFrame | None:
    frames: list[pd.DataFrame] = []

    if industry_scores_df is not None and not industry_scores_df.empty:
        score_columns = ["industry"]
        score_columns.extend(
            column
            for column in ["industry_risk_score", "avg_dscr", "avg_margin", "avg_utilisation"]
            if column in industry_scores_df.columns
        )
        frames.append(industry_scores_df[score_columns].copy())

    if industry_downturn_df is not None and not industry_downturn_df.empty:
        overlay = industry_downturn_df.copy()
        overlay = overlay.rename(
            columns={
                "pd_overlay_multiplier": "industry_pd_overlay_multiplier",
                "lgd_overlay_addon": "industry_lgd_overlay_addon",
            }
        )
        overlay_columns = ["industry"]
        overlay_columns.extend(
            column
            for column in ["industry_pd_overlay_multiplier", "industry_lgd_overlay_addon"]
            if column in overlay.columns
        )
        frames.append(overlay[overlay_columns])

    if not frames:
        return None

    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on="industry", how="outer", validate="one_to_one")
    return merged


def build_expected_loss_dataset(
    portfolio_df: pd.DataFrame,
    pd_df: pd.DataFrame,
    lgd_df: pd.DataFrame,
    ead_df: pd.DataFrame | None = None,
    industry_scores_df: pd.DataFrame | None = None,
    industry_downturn_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if "facility_id" not in portfolio_df.columns:
        raise ValueError("Portfolio input must include facility_id")

    merged = portfolio_df.merge(_prepare_pd_inputs(pd_df), on="facility_id", how="left", validate="one_to_one")
    merged = merged.merge(_prepare_lgd_inputs(lgd_df), on="facility_id", how="left", validate="one_to_one")

    industry_inputs = _prepare_industry_inputs(industry_scores_df, industry_downturn_df)
    if industry_inputs is not None and "industry" in merged.columns:
        merged = merged.merge(industry_inputs, on="industry", how="left", validate="many_to_one")

    if merged["pd_final"].isna().any():
        missing_ids = merged.loc[merged["pd_final"].isna(), "facility_id"].head(5).tolist()
        raise ValueError(f"Missing PD rows for facility_id values such as: {missing_ids}")
    if merged["lgd_final"].isna().any():
        missing_ids = merged.loc[merged["lgd_final"].isna(), "facility_id"].head(5).tolist()
        raise ValueError(f"Missing LGD rows for facility_id values such as: {missing_ids}")

    merged = add_ead_columns(merged)
    merged["ead_model_source"] = "internal"

    if ead_df is not None and not ead_df.empty:
        merged = merged.merge(_prepare_ead_inputs(ead_df), on="facility_id", how="left", validate="one_to_one")
        external_mask = merged.get("ead_external", pd.Series(pd.NA, index=merged.index)).notna()

        if external_mask.any():
            for target, source in [
                ("limit_amount", "limit_amount_external"),
                ("drawn_balance", "drawn_balance_external"),
                ("undrawn_amount", "undrawn_amount_external"),
                ("utilisation", "utilisation_external"),
                ("ccf_base", "ccf_base_external"),
                ("ccf_applied", "ccf_base_external"),
                ("ccf_downturn", "ccf_downturn_external"),
                ("ead", "ead_external"),
                ("ead_downturn", "ead_downturn_external"),
            ]:
                if source in merged.columns:
                    merged.loc[external_mask, target] = merged.loc[external_mask, source]
            merged.loc[external_mask, "ead_model_source"] = "external"

    merged["expected_loss"] = (merged["pd_final"] * merged["lgd_final"] * merged["ead"]).round(2)
    merged["el_rate"] = (merged["expected_loss"] / merged["ead"].replace(0, pd.NA)).fillna(0.0).round(6)

    optional_columns = {
        "borrower_name": pd.NA,
        "region": pd.NA,
        "loan_type": pd.NA,
        "security_type": pd.NA,
        "loan_term_months": pd.NA,
        "limit_amount": pd.NA,
        "drawn_balance": pd.NA,
        "undrawn_amount": pd.NA,
        "ccf_bucket": pd.NA,
        "ccf_base": pd.NA,
        "ccf_applied": pd.NA,
        "ccf_downturn": pd.NA,
        "ead_downturn": pd.NA,
        "utilisation": pd.NA,
        "interest_rate": pd.NA,
        "annual_revenue": pd.NA,
        "ebitda": pd.NA,
        "dscr": pd.NA,
        "arrears_days": pd.NA,
        "watchlist_flag": pd.NA,
        "internal_risk_grade": pd.NA,
        "borrower_strength": pd.NA,
        "industry_risk_score": pd.NA,
        "industry_pd_overlay_multiplier": pd.NA,
        "industry_lgd_overlay_addon": pd.NA,
    }
    for column, default in optional_columns.items():
        if column not in merged.columns:
            merged[column] = default

    ordered_columns = [
        "facility_id",
        "borrower_id",
        "borrower_name",
        "product_type",
        "industry",
        "region",
        "loan_type",
        "security_type",
        "loan_term_months",
        "limit_amount",
        "drawn_balance",
        "undrawn_amount",
        "utilisation",
        "ccf_bucket",
        "ccf_base",
        "ccf_applied",
        "ccf_downturn",
        "ead",
        "ead_downturn",
        "ead_model_source",
        "interest_rate",
        "score",
        "score_band",
        "risk_grade",
        "pd_model_stream",
        "pd_final",
        "lgd_final",
        "expected_loss",
        "el_rate",
        "annual_revenue",
        "ebitda",
        "dscr",
        "arrears_days",
        "watchlist_flag",
        "internal_risk_grade",
        "borrower_strength",
        "industry_risk_score",
        "industry_pd_overlay_multiplier",
        "industry_lgd_overlay_addon",
        "conduct_classification",
        "default_horizon_months",
        "pd_model_name",
        "pd_model_version",
        "as_of_date",
        "lgd_base",
        "lgd_adj_lvr",
        "lgd_adj_stage",
        "lgd_adj_industry",
        "lgd_adj_pd_band",
        "lgd_adj_dscr",
        "lgd_adj_conduct",
        "lgd_adjusted",
        "downturn_scalar",
        "lgd_downturn",
    ]
    return merged[ordered_columns]
