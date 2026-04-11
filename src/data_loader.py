from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from .config import (
    AS_OF_DATE,
    DEFAULT_INPUT_FILES,
    INDUSTRY_SETTINGS,
    INPUT_DIR,
    N_FACILITIES,
    PRODUCT_SETTINGS,
    RANDOM_SEED,
    REGION_FACTORS,
    RISK_GRADE_PD_LOOKUP,
    RISK_GRADE_TO_SCORE_BAND,
    SIBLING_INPUT_CANDIDATES,
    STRESS_SCENARIOS,
)
from .utils import ensure_directories, save_dataframe


REQUIRED_PORTFOLIO_COLUMNS = (
    "facility_id",
    "borrower_id",
    "product_type",
    "industry",
    "limit_amount",
    "drawn_balance",
)


def _resolve_input_dir(input_dir: str | Path | None) -> Path:
    return Path(input_dir) if input_dir is not None else INPUT_DIR


def _source_row(
    strategy: str,
    source_key: str,
    status: str,
    detail: str,
    path: str | Path | None = None,
    facility_count: int | None = None,
    shared_facility_count: int | None = None,
) -> dict[str, object]:
    return {
        "strategy": strategy,
        "source_key": source_key,
        "status": status,
        "detail": detail,
        "path": str(path) if path is not None else "",
        "facility_count": facility_count,
        "shared_facility_count": shared_facility_count,
    }


def _find_existing_candidate(source_key: str) -> Path | None:
    for candidate in SIBLING_INPUT_CANDIDATES.get(source_key, ()):
        if candidate.exists():
            return candidate
    return None


def _normalise_facility_frame(df: pd.DataFrame, fallback_id_column: str | None = None) -> pd.DataFrame:
    out = df.copy()
    if "facility_id" not in out.columns and fallback_id_column and fallback_id_column in out.columns:
        out = out.rename(columns={fallback_id_column: "facility_id"})
    if "facility_id" in out.columns:
        out["facility_id"] = out["facility_id"].astype(str)
    return out


def _normalise_portfolio_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = _normalise_facility_frame(df)
    rename_map = {
        "limit": "limit_amount",
        "drawn": "drawn_balance",
        "revenue": "annual_revenue",
        "maturity": "maturity_years",
    }
    out = out.rename(columns={key: value for key, value in rename_map.items() if key in out.columns})

    if "maturity_years" in out.columns and "loan_term_months" not in out.columns:
        maturity_years = pd.to_numeric(out["maturity_years"], errors="coerce")
        out["loan_term_months"] = (maturity_years * 12).round().astype("Int64")

    defaults = {
        "borrower_name": pd.NA,
        "region": pd.NA,
        "interest_rate": pd.NA,
        "loan_type": pd.NA,
        "security_type": pd.NA,
        "property_value": pd.NA,
        "current_lvr": pd.NA,
        "arrears_days": pd.NA,
        "internal_risk_grade": pd.NA,
        "watchlist_flag": pd.NA,
        "borrower_strength": pd.NA,
    }
    for column, default in defaults.items():
        if column not in out.columns:
            out[column] = default
    return out


def _candidate_path_string(paths: tuple[Path, ...] | list[Path]) -> str:
    return "; ".join(str(path) for path in paths)


def _load_aligned_portfolio_anchor(
    target_dir: Path,
    shared_ids: set[str],
    rows: list[dict[str, object]],
    pd_df: pd.DataFrame,
    ead_df: pd.DataFrame | None,
) -> pd.DataFrame | None:
    local_candidates: list[Path] = []
    local_portfolio_path = target_dir / DEFAULT_INPUT_FILES["portfolio"].name
    if local_portfolio_path.exists():
        local_candidates.append(local_portfolio_path)

    sibling_candidates = [path for path in SIBLING_INPUT_CANDIDATES.get("portfolio_sample", ()) if path.exists()]

    for path in [*local_candidates, *sibling_candidates]:
        portfolio_df = _normalise_portfolio_frame(pd.read_csv(path))
        missing_columns = [column for column in REQUIRED_PORTFOLIO_COLUMNS if column not in portfolio_df.columns]
        if missing_columns:
            rows.append(
                _source_row(
                    "sibling_bundle",
                    "portfolio",
                    "rejected",
                    f"Portfolio candidate is missing required columns: {missing_columns}",
                    path=path,
                    facility_count=len(portfolio_df),
                )
            )
            continue

        overlap = shared_ids & set(portfolio_df["facility_id"])
        rows.append(
            _source_row(
                "sibling_bundle",
                "portfolio",
                "loaded" if overlap else "available_not_aligned",
                "Loaded aligned sibling/local portfolio candidate." if overlap else "Portfolio candidate exists but does not align to the sibling facility universe.",
                path=path,
                facility_count=len(portfolio_df),
                shared_facility_count=len(overlap),
            )
        )
        if overlap:
            return portfolio_df[portfolio_df["facility_id"].isin(shared_ids)].copy()

    if ead_df is not None and not ead_df.empty:
        anchor = _normalise_portfolio_frame(ead_df)
        if "borrower_id" not in anchor.columns and "borrower_id" in pd_df.columns:
            anchor = anchor.merge(pd_df[["facility_id", "borrower_id"]], on="facility_id", how="left", validate="one_to_one")
        enrich_columns = [column for column in ["facility_id", "borrower_id", "product_type", "industry", "dscr"] if column in pd_df.columns]
        if enrich_columns:
            anchor = anchor.merge(pd_df[enrich_columns], on="facility_id", how="left", validate="one_to_one", suffixes=("", "_pd"))
            for column in ["borrower_id", "product_type", "industry", "dscr"]:
                pd_column = f"{column}_pd"
                if pd_column in anchor.columns:
                    if column not in anchor.columns:
                        anchor[column] = anchor[pd_column]
                    else:
                        anchor[column] = anchor[column].fillna(anchor[pd_column])
                    anchor = anchor.drop(columns=[pd_column])
        missing_columns = [column for column in REQUIRED_PORTFOLIO_COLUMNS if column not in anchor.columns]
        if not missing_columns:
            anchor = anchor[anchor["facility_id"].isin(shared_ids)].copy()
            rows.append(
                _source_row(
                    "sibling_bundle",
                    "portfolio",
                    "generated",
                    "Built portfolio anchor from aligned sibling EAD/PD exports.",
                    facility_count=len(anchor),
                    shared_facility_count=len(anchor),
                )
            )
            return anchor

    return None


def _assign_risk_grade(risk_score: float) -> str:
    if risk_score < 0.22:
        return "RG1"
    if risk_score < 0.30:
        return "RG2"
    if risk_score < 0.40:
        return "RG3"
    if risk_score < 0.54:
        return "RG4"
    return "RG5"


def _borrower_strength(risk_grade: str, watchlist_flag: int, arrears_days: int) -> str:
    if watchlist_flag or arrears_days >= 45 or risk_grade in {"RG4", "RG5"}:
        return "weak"
    if risk_grade == "RG3":
        return "average"
    return "strong"


def _security_bucket(product_type: str, security_type: str) -> str:
    if product_type == "Property Backed Loan":
        return "residential" if "Residential" in security_type else "commercial"
    if product_type == "Overdraft / Revolving Working Capital":
        return "unsecured" if security_type == "Unsecured" else "secured"
    if security_type == "Unsecured":
        return "unsecured"
    if security_type == "Receivables Security":
        return "partially_secured"
    return "secured"


def _base_lgd(product_type: str, security_bucket: str) -> float:
    if product_type == "Property Backed Loan":
        return 0.20 if security_bucket == "residential" else 0.28
    if product_type == "Overdraft / Revolving Working Capital":
        return 0.55 if security_bucket == "secured" else 0.68
    if security_bucket == "secured":
        return 0.42
    if security_bucket == "partially_secured":
        return 0.50
    return 0.60


def _build_portfolio_dataset(n_facilities: int = N_FACILITIES, seed: int = RANDOM_SEED) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    product_names = tuple(PRODUCT_SETTINGS.keys())
    product_weights = np.array([0.40, 0.32, 0.28], dtype=float)
    industry_names = tuple(INDUSTRY_SETTINGS.keys())
    region_names = tuple(REGION_FACTORS.keys())

    records: list[dict] = []
    borrower_mod = max(1, n_facilities // 2)
    for facility_number in range(1, n_facilities + 1):
        product_type = rng.choice(product_names, p=product_weights / product_weights.sum())
        product_settings = PRODUCT_SETTINGS[product_type]

        industry = rng.choice(industry_names)
        industry_settings = INDUSTRY_SETTINGS[industry]
        region = rng.choice(region_names)

        quality = rng.normal(loc=0.0, scale=1.0)
        industry_factor = float(industry_settings["risk_factor"])
        product_factor = float(product_settings["risk_factor"])
        region_factor = float(REGION_FACTORS[region])

        security_type = rng.choice(
            product_settings["security_types"],
            p=np.array(product_settings["security_weights"], dtype=float),
        )
        limit_amount = round(rng.uniform(*product_settings["limit_range"]), 2)
        drawn_pct = float(rng.uniform(*product_settings["drawn_range"]))
        drawn_balance = round(limit_amount * drawn_pct, 2)
        loan_term_months = int(rng.choice(product_settings["term_options"]))

        revenue_low, revenue_high = industry_settings["revenue_range"]
        annual_revenue = round(
            np.clip(
                rng.uniform(revenue_low, revenue_high) * (1.0 + quality * 0.12),
                revenue_low * 0.60,
                revenue_high * 1.20,
            ),
            2,
        )
        margin_low, margin_high = industry_settings["ebitda_margin_range"]
        ebitda_margin = float(np.clip(rng.uniform(margin_low, margin_high) + quality * 0.015, 0.04, 0.30))
        ebitda = round(annual_revenue * ebitda_margin, 2)

        dscr = float(
            np.clip(
                1.55 + quality * 0.22 - product_factor * 0.55 - industry_factor * 0.35 + rng.normal(0, 0.08),
                0.75,
                2.50,
            )
        )
        arrears_days = int(
            np.clip(
                round(rng.normal(8 + product_factor * 55 + industry_factor * 30 + max(-quality, 0) * 20, 14)),
                0,
                120,
            )
        )

        property_value = np.nan
        current_lvr = np.nan
        if product_type == "Property Backed Loan":
            current_lvr = float(
                np.clip(
                    0.54 + product_factor * 0.20 + industry_factor * 0.08 + max(-quality, 0) * 0.06 + rng.normal(0, 0.05),
                    0.38,
                    0.92,
                )
            )
            property_value = round(drawn_balance / max(current_lvr, 0.25), 2)

        watchlist_flag = int(
            arrears_days >= 60
            or dscr < 1.00
            or (product_type == "Property Backed Loan" and current_lvr >= 0.82)
            or rng.random() < (0.03 + industry_factor * 0.03 + max(-quality, 0) * 0.06)
        )

        risk_score = float(
            np.clip(
                0.12
                + product_factor * 0.45
                + industry_factor * 0.35
                + max(1.25 - dscr, 0) * 0.28
                + (arrears_days / 120.0) * 0.22
                + watchlist_flag * 0.12
                + (max((current_lvr if not np.isnan(current_lvr) else 0.62) - 0.65, 0) * 0.20)
                + max(region_factor, 0) * 0.20
                + rng.normal(0, 0.025),
                0.05,
                0.95,
            )
        )
        internal_risk_grade = _assign_risk_grade(risk_score)
        borrower_strength = _borrower_strength(internal_risk_grade, watchlist_flag, arrears_days)
        risk_addon = {"RG1": 0.000, "RG2": 0.005, "RG3": 0.011, "RG4": 0.020, "RG5": 0.035}[internal_risk_grade]
        interest_rate = round(product_settings["interest_base"] + risk_addon + rng.normal(0, 0.004), 4)

        borrower_number = ((facility_number - 1) % borrower_mod) + 1
        records.append(
            {
                "facility_id": f"FAC-{facility_number:05d}",
                "borrower_id": f"BOR-{borrower_number:04d}",
                "borrower_name": f"Borrower {borrower_number:04d}",
                "product_type": product_type,
                "industry": industry,
                "region": region,
                "limit_amount": limit_amount,
                "drawn_balance": drawn_balance,
                "interest_rate": interest_rate,
                "loan_type": product_settings["loan_type"],
                "security_type": security_type,
                "loan_term_months": loan_term_months,
                "property_value": property_value,
                "current_lvr": round(current_lvr, 4) if not np.isnan(current_lvr) else np.nan,
                "annual_revenue": annual_revenue,
                "ebitda": ebitda,
                "dscr": round(dscr, 3),
                "arrears_days": arrears_days,
                "internal_risk_grade": internal_risk_grade,
                "watchlist_flag": watchlist_flag,
                "borrower_strength": borrower_strength,
            }
        )

    return pd.DataFrame.from_records(records)


def _build_pd_final_dataset(portfolio_df: pd.DataFrame) -> pd.DataFrame:
    out = portfolio_df[
        [
            "facility_id",
            "borrower_id",
            "borrower_name",
            "product_type",
            "industry",
            "security_type",
            "internal_risk_grade",
            "watchlist_flag",
            "arrears_days",
            "current_lvr",
        ]
    ].copy()

    out["pd_model_stream"] = np.where(out["product_type"] == "Property Backed Loan", "property", "cashflow")
    out["property_segment"] = np.where(out["product_type"] == "Property Backed Loan", out["security_type"], pd.NA)
    out["risk_grade"] = out["internal_risk_grade"]
    out["score_band"] = out["risk_grade"].map(RISK_GRADE_TO_SCORE_BAND)
    score_base = out["risk_grade"].map({"RG1": 780, "RG2": 730, "RG3": 675, "RG4": 610, "RG5": 545})
    score_adjustment = (out["watchlist_flag"] * -25) + np.clip(out["arrears_days"] - 15, 0, 90) * -0.55
    out["score"] = (score_base + score_adjustment).clip(420, 820).round(0).astype(int)

    pd_multiplier = (
        1.0
        + out["watchlist_flag"] * 0.18
        + (out["arrears_days"] >= 30).astype(float) * 0.10
        + (out["product_type"] == "Overdraft / Revolving Working Capital").astype(float) * 0.12
        - (out["product_type"] == "Property Backed Loan").astype(float) * 0.08
        + out["industry"].map(lambda value: INDUSTRY_SETTINGS[value]["risk_factor"]) * 0.35
        + np.where(out["current_lvr"].fillna(0) >= 0.80, 0.08, 0.00)
    )
    out["pd_final"] = (out["risk_grade"].map(RISK_GRADE_PD_LOOKUP) * pd_multiplier).clip(0.005, 0.35).round(4)
    out["default_horizon_months"] = 12
    out["pd_model_name"] = np.where(out["pd_model_stream"] == "property", "property_logistic_scorecard", "cashflow_logistic_scorecard")
    out["pd_model_version"] = "v1.0"
    out["as_of_date"] = AS_OF_DATE

    return out[
        [
            "facility_id",
            "borrower_id",
            "borrower_name",
            "product_type",
            "pd_model_stream",
            "industry",
            "property_segment",
            "score",
            "score_band",
            "risk_grade",
            "pd_final",
            "default_horizon_months",
            "pd_model_name",
            "pd_model_version",
            "as_of_date",
        ]
    ]


def _build_lgd_final_dataset(portfolio_df: pd.DataFrame, pd_df: pd.DataFrame) -> pd.DataFrame:
    merged = portfolio_df.merge(pd_df[["facility_id", "score_band"]], on="facility_id", how="left")
    conduct_classification = np.select(
        [
            (merged["watchlist_flag"] == 1) | (merged["arrears_days"] >= 60),
            merged["arrears_days"] >= 15,
        ],
        ["Red", "Amber"],
        default="Green",
    )

    security_bucket = merged.apply(lambda row: _security_bucket(row["product_type"], row["security_type"]), axis=1)
    lgd_base = [_base_lgd(product_type, bucket) for product_type, bucket in zip(merged["product_type"], security_bucket, strict=False)]
    lgd_adj_lvr = np.where(merged["current_lvr"].fillna(0) >= 0.80, 0.06, np.where(merged["current_lvr"].fillna(0) >= 0.65, 0.03, 0.00))
    industry_factor = merged["industry"].map(lambda value: INDUSTRY_SETTINGS[value]["risk_factor"])
    lgd_adj_industry = np.where(industry_factor >= 0.20, 0.02, np.where(industry_factor >= 0.16, 0.01, 0.00))
    lgd_adj_pd_band = merged["score_band"].map({"A": -0.03, "B": -0.01, "C": 0.00, "D": 0.02, "E": 0.05}).fillna(0.00)
    lgd_adj_pd_band = np.where(merged["product_type"] == "Property Backed Loan", 0.00, lgd_adj_pd_band)
    lgd_adj_dscr = np.where(merged["dscr"] < 1.10, 0.03, np.where(merged["dscr"] < 1.25, 0.01, 0.00))
    lgd_adj_conduct = pd.Series(conduct_classification).map({"Green": 0.00, "Amber": 0.01, "Red": 0.03}).to_numpy()
    downturn_scalar = np.where(merged["product_type"] == "Property Backed Loan", 1.08, 1.10)

    lgd_adjusted = np.array(lgd_base) + lgd_adj_lvr + lgd_adj_industry + lgd_adj_pd_band + lgd_adj_dscr + lgd_adj_conduct
    lgd_downturn = lgd_adjusted * downturn_scalar
    lgd_final = np.clip(lgd_downturn, 0.08, 0.95)
    property_type = np.where(
        merged["product_type"] == "Property Backed Loan",
        np.where(merged["security_type"] == "Residential Investment Property", "Residential", "Commercial"),
        pd.NA,
    )

    return pd.DataFrame(
        {
            "loan_id": merged["facility_id"],
            "source_product": merged["product_type"],
            "source_loan_id": merged["facility_id"],
            "product_type": merged["product_type"],
            "security_type": security_bucket,
            "property_type": property_type,
            "property_value": merged["property_value"],
            "current_lvr": merged["current_lvr"],
            "loan_stage": pd.NA,
            "industry": merged["industry"],
            "ead": merged["drawn_balance"],
            "pd_score_band": merged["score_band"],
            "dscr": merged["dscr"],
            "conduct_classification": conduct_classification,
            "lgd_base": np.round(lgd_base, 4),
            "lgd_adj_lvr": np.round(lgd_adj_lvr, 4),
            "lgd_adj_stage": 0.0,
            "lgd_adj_industry": np.round(lgd_adj_industry, 4),
            "lgd_adj_pd_band": np.round(lgd_adj_pd_band, 4),
            "lgd_adj_dscr": np.round(lgd_adj_dscr, 4),
            "lgd_adj_conduct": np.round(lgd_adj_conduct, 4),
            "lgd_adjusted": np.round(lgd_adjusted, 4),
            "downturn_scalar": np.round(downturn_scalar, 4),
            "lgd_downturn": np.round(lgd_downturn, 4),
            "lgd_final": np.round(lgd_final, 4),
        }
    )


def _build_downturn_overlay_table() -> pd.DataFrame:
    return pd.DataFrame(STRESS_SCENARIOS)


def build_demo_input_tables(n_facilities: int = N_FACILITIES, seed: int = RANDOM_SEED) -> dict[str, pd.DataFrame]:
    portfolio_df = _build_portfolio_dataset(n_facilities=n_facilities, seed=seed)
    pd_df = _build_pd_final_dataset(portfolio_df)
    lgd_df = _build_lgd_final_dataset(portfolio_df, pd_df)
    overlays_df = _build_downturn_overlay_table()
    return {
        "portfolio": portfolio_df,
        "pd_final": pd_df,
        "lgd_final": lgd_df,
        "downturn_overlays": overlays_df,
    }


def _persist_demo_bundle(target_dir: Path, tables: dict[str, pd.DataFrame]) -> None:
    save_dataframe(tables["portfolio"], target_dir / DEFAULT_INPUT_FILES["portfolio"].name)
    save_dataframe(tables["pd_final"], target_dir / DEFAULT_INPUT_FILES["pd_final"].name)
    save_dataframe(tables["lgd_final"], target_dir / DEFAULT_INPUT_FILES["lgd_final"].name)
    save_dataframe(tables["downturn_overlays"], target_dir / DEFAULT_INPUT_FILES["downturn_overlays"].name)


def stage_demo_inputs(
    input_dir: str | Path | None = None,
    overwrite: bool = False,
    n_facilities: int = N_FACILITIES,
    seed: int = RANDOM_SEED,
) -> dict[str, Path]:
    target_dir = _resolve_input_dir(input_dir)
    ensure_directories(target_dir)
    file_map = {
        "portfolio": target_dir / DEFAULT_INPUT_FILES["portfolio"].name,
        "pd_final": target_dir / DEFAULT_INPUT_FILES["pd_final"].name,
        "lgd_final": target_dir / DEFAULT_INPUT_FILES["lgd_final"].name,
        "downturn_overlays": target_dir / DEFAULT_INPUT_FILES["downturn_overlays"].name,
    }
    if overwrite or all(not path.exists() for path in file_map.values()):
        _persist_demo_bundle(target_dir, build_demo_input_tables(n_facilities=n_facilities, seed=seed))
    return file_map


def _try_load_sibling_bundle(target_dir: Path) -> tuple[dict[str, pd.DataFrame | None] | None, list[dict[str, object]]]:
    rows: list[dict[str, object]] = []
    pd_path = _find_existing_candidate("pd_final")
    lgd_path = _find_existing_candidate("lgd_final")
    if pd_path is None or lgd_path is None:
        missing_sources = [name for name, path in {"pd_final": pd_path, "lgd_final": lgd_path}.items() if path is None]
        rows.append(
            _source_row(
                "sibling_bundle",
                "strategy",
                "rejected",
                f"Missing sibling source files: {missing_sources}",
            )
        )
        return None, rows

    pd_df = _normalise_facility_frame(pd.read_csv(pd_path))
    lgd_df = _normalise_facility_frame(pd.read_csv(lgd_path), fallback_id_column="loan_id")

    rows.extend(
        [
            _source_row("sibling_bundle", "pd_final", "loaded", "Loaded sibling PD export.", path=pd_path, facility_count=len(pd_df)),
            _source_row("sibling_bundle", "lgd_final", "loaded", "Loaded sibling LGD export.", path=lgd_path, facility_count=len(lgd_df)),
        ]
    )

    pd_ids = set(pd_df["facility_id"]) if "facility_id" in pd_df.columns else set()
    lgd_ids = set(lgd_df["facility_id"]) if "facility_id" in lgd_df.columns else set()
    shared_ids = pd_ids & lgd_ids

    if not shared_ids:
        rows.append(
            _source_row(
                "sibling_bundle",
                "strategy",
                "rejected",
                "Sibling PD and LGD exports do not share facility_id values.",
                shared_facility_count=0,
            )
        )
        return None, rows

    ead_df: pd.DataFrame | None = None
    ead_path = _find_existing_candidate("ead_final")
    if ead_path is not None:
        ead_candidate = _normalise_facility_frame(pd.read_csv(ead_path))
        ead_shared = shared_ids & set(ead_candidate["facility_id"]) if "facility_id" in ead_candidate.columns else set()
        rows.append(
            _source_row(
                "sibling_bundle",
                "ead_final",
                "loaded" if ead_shared else "available_not_aligned",
                "Loaded sibling EAD export." if ead_shared else "Sibling EAD export exists but does not align to the selected facility universe.",
                path=ead_path,
                facility_count=len(ead_candidate),
                shared_facility_count=len(ead_shared),
            )
        )
        if ead_shared:
            ead_df = ead_candidate[ead_candidate["facility_id"].isin(shared_ids)].copy()

    filtered_pd = pd_df[pd_df["facility_id"].isin(shared_ids)].copy()
    filtered_lgd = lgd_df[lgd_df["facility_id"].isin(shared_ids)].copy()
    filtered_portfolio = _load_aligned_portfolio_anchor(target_dir, shared_ids, rows, filtered_pd, ead_df)
    if filtered_portfolio is None:
        candidate_paths = [target_dir / DEFAULT_INPUT_FILES["portfolio"].name]
        candidate_paths.extend(path for path in SIBLING_INPUT_CANDIDATES.get("portfolio_sample", ()) if path.exists())
        rows.append(
            _source_row(
                "sibling_bundle",
                "strategy",
                "rejected",
                "No local or sibling portfolio source could be aligned to the shared sibling facility universe.",
                path=_candidate_path_string(candidate_paths),
                shared_facility_count=len(shared_ids),
            )
        )
        return None, rows

    industry_scores = None
    industry_scores_path = _find_existing_candidate("industry_scores")
    if industry_scores_path is not None:
        industry_scores = pd.read_csv(industry_scores_path)
        rows.append(
            _source_row(
                "sibling_bundle",
                "industry_scores",
                "loaded",
                "Loaded sibling industry risk scores.",
                path=industry_scores_path,
                facility_count=len(industry_scores),
            )
        )

    industry_downturns = None
    industry_downturns_path = _find_existing_candidate("industry_downturns")
    if industry_downturns_path is not None:
        industry_downturns = pd.read_csv(industry_downturns_path)
        rows.append(
            _source_row(
                "sibling_bundle",
                "industry_downturns",
                "loaded",
                "Loaded sibling industry downturn overlays.",
                path=industry_downturns_path,
                facility_count=len(industry_downturns),
            )
        )

    local_downturn_path = target_dir / DEFAULT_INPUT_FILES["downturn_overlays"].name
    if local_downturn_path.exists():
        downturn_overlays = pd.read_csv(local_downturn_path)
        rows.append(
            _source_row(
                "sibling_bundle",
                "downturn_overlays",
                "loaded",
                "Loaded local scenario overlay table.",
                path=local_downturn_path,
                facility_count=len(downturn_overlays),
            )
        )
    else:
        downturn_overlays = _build_downturn_overlay_table()
        rows.append(
            _source_row(
                "sibling_bundle",
                "downturn_overlays",
                "generated",
                "Generated default scenario overlay table because no local scenario file was supplied.",
                facility_count=len(downturn_overlays),
            )
        )

    rows.append(
        _source_row(
            "sibling_bundle",
            "strategy",
            "selected",
            "Using aligned sibling PD/LGD/EAD exports with a local or sibling portfolio anchor.",
            shared_facility_count=len(shared_ids),
        )
    )
    return {
        "portfolio": filtered_portfolio.reset_index(drop=True),
        "pd_final": filtered_pd.reset_index(drop=True),
        "lgd_final": filtered_lgd.reset_index(drop=True),
        "ead_final": ead_df.reset_index(drop=True) if ead_df is not None else None,
        "industry_scores": industry_scores,
        "industry_downturns": industry_downturns,
        "downturn_overlays": downturn_overlays.reset_index(drop=True),
    }, rows


def _try_load_local_bundle(target_dir: Path) -> tuple[dict[str, pd.DataFrame | None] | None, list[dict[str, object]]]:
    rows: list[dict[str, object]] = []
    file_map = {
        "portfolio": target_dir / DEFAULT_INPUT_FILES["portfolio"].name,
        "pd_final": target_dir / DEFAULT_INPUT_FILES["pd_final"].name,
        "lgd_final": target_dir / DEFAULT_INPUT_FILES["lgd_final"].name,
        "downturn_overlays": target_dir / DEFAULT_INPUT_FILES["downturn_overlays"].name,
    }
    missing = [key for key, path in file_map.items() if not path.exists()]
    if missing:
        rows.append(
            _source_row(
                "local_input_bundle",
                "strategy",
                "skipped",
                f"Missing local input files: {missing}",
            )
        )
        return None, rows

    bundle: dict[str, pd.DataFrame | None] = {
        "portfolio": _normalise_portfolio_frame(pd.read_csv(file_map["portfolio"])),
        "pd_final": _normalise_facility_frame(pd.read_csv(file_map["pd_final"])),
        "lgd_final": _normalise_facility_frame(pd.read_csv(file_map["lgd_final"]), fallback_id_column="loan_id"),
        "downturn_overlays": pd.read_csv(file_map["downturn_overlays"]),
        "ead_final": None,
        "industry_scores": None,
        "industry_downturns": None,
    }

    optional_local_files = {
        "ead_final": target_dir / "ead_by_facility.csv",
        "industry_scores": target_dir / "industry_risk_score_table.csv",
        "industry_downturns": target_dir / "downturn_overlay_table.csv",
    }
    for key, path in optional_local_files.items():
        if path.exists():
            frame = pd.read_csv(path)
            if key == "ead_final":
                frame = _normalise_facility_frame(frame)
            bundle[key] = frame

    rows.extend(
        [
            _source_row("local_input_bundle", "strategy", "selected", "Using local input bundle staged under data/input or the provided input_dir."),
            _source_row("local_input_bundle", "portfolio", "loaded", "Loaded local portfolio master.", path=file_map["portfolio"], facility_count=len(bundle["portfolio"])),
            _source_row("local_input_bundle", "pd_final", "loaded", "Loaded local PD final-layer file.", path=file_map["pd_final"], facility_count=len(bundle["pd_final"])),
            _source_row("local_input_bundle", "lgd_final", "loaded", "Loaded local LGD final-layer file.", path=file_map["lgd_final"], facility_count=len(bundle["lgd_final"])),
            _source_row("local_input_bundle", "downturn_overlays", "loaded", "Loaded local scenario overlay table.", path=file_map["downturn_overlays"], facility_count=len(bundle["downturn_overlays"])),
        ]
    )
    return bundle, rows


def load_input_tables(
    input_dir: str | Path | None = None,
    refresh_demo: bool = False,
    prefer_sibling_inputs: bool = True,
    strict_sibling_inputs: bool = False,
) -> dict[str, pd.DataFrame | None]:
    target_dir = _resolve_input_dir(input_dir)
    ensure_directories(target_dir)
    rows: list[dict[str, object]] = []

    if refresh_demo:
        demo_tables = build_demo_input_tables()
        _persist_demo_bundle(target_dir, demo_tables)
        rows.append(_source_row("demo_generated", "strategy", "selected", "Refreshed and persisted aligned demo inputs."))
        rows.extend(
            [
                _source_row("demo_generated", "portfolio", "generated", "Generated demo portfolio.", path=target_dir / DEFAULT_INPUT_FILES["portfolio"].name, facility_count=len(demo_tables["portfolio"])),
                _source_row("demo_generated", "pd_final", "generated", "Generated demo PD final layer.", path=target_dir / DEFAULT_INPUT_FILES["pd_final"].name, facility_count=len(demo_tables["pd_final"])),
                _source_row("demo_generated", "lgd_final", "generated", "Generated demo LGD final layer.", path=target_dir / DEFAULT_INPUT_FILES["lgd_final"].name, facility_count=len(demo_tables["lgd_final"])),
                _source_row("demo_generated", "downturn_overlays", "generated", "Generated demo scenario overlays.", path=target_dir / DEFAULT_INPUT_FILES["downturn_overlays"].name, facility_count=len(demo_tables["downturn_overlays"])),
            ]
        )
        return {
            **demo_tables,
            "ead_final": None,
            "industry_scores": None,
            "industry_downturns": None,
            "input_source_report": pd.DataFrame.from_records(rows),
            "selected_input_strategy": "demo_generated",
        }

    if prefer_sibling_inputs:
        sibling_bundle, sibling_rows = _try_load_sibling_bundle(target_dir)
        rows.extend(sibling_rows)
        if sibling_bundle is not None:
            return {
                **sibling_bundle,
                "input_source_report": pd.DataFrame.from_records(rows),
                "selected_input_strategy": "sibling_bundle",
            }
        if strict_sibling_inputs:
            raise ValueError("Strict sibling input mode was requested, but sibling repo exports could not be reconciled to a shared sibling portfolio universe.")

    local_bundle, local_rows = _try_load_local_bundle(target_dir)
    rows.extend(local_rows)
    if local_bundle is not None:
        return {
            **local_bundle,
            "input_source_report": pd.DataFrame.from_records(rows),
            "selected_input_strategy": "local_input_bundle",
        }

    demo_tables = build_demo_input_tables()
    rows.append(_source_row("demo_generated", "strategy", "selected", "Generated an in-memory demo bundle because no coherent local or sibling input bundle was available."))
    rows.extend(
        [
            _source_row("demo_generated", "portfolio", "generated", "Generated demo portfolio in memory.", facility_count=len(demo_tables["portfolio"])),
            _source_row("demo_generated", "pd_final", "generated", "Generated demo PD final layer in memory.", facility_count=len(demo_tables["pd_final"])),
            _source_row("demo_generated", "lgd_final", "generated", "Generated demo LGD final layer in memory.", facility_count=len(demo_tables["lgd_final"])),
            _source_row("demo_generated", "downturn_overlays", "generated", "Generated demo scenario overlays in memory.", facility_count=len(demo_tables["downturn_overlays"])),
        ]
    )
    return {
        **demo_tables,
        "ead_final": None,
        "industry_scores": None,
        "industry_downturns": None,
        "input_source_report": pd.DataFrame.from_records(rows),
        "selected_input_strategy": "demo_generated",
    }
