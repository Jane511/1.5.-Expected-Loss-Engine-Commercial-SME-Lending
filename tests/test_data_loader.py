from __future__ import annotations

from pathlib import Path

import pandas as pd

import src.data_loader as data_loader


def _write_csv(path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def test_load_input_tables_uses_sibling_bundle_when_facilities_align(monkeypatch) -> None:
    root = Path("tests") / "_artifacts_loader" / "aligned"
    input_dir = root / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(
        input_dir / "portfolio_input.csv",
        [
            {
                "facility_id": "FAC-1",
                "borrower_id": "BOR-1",
                "product_type": "SME Cash Flow Term Loan",
                "industry": "Manufacturing",
                "limit_amount": 1_000_000,
                "drawn_balance": 800_000,
            },
            {
                "facility_id": "FAC-2",
                "borrower_id": "BOR-2",
                "product_type": "Property Backed Loan",
                "industry": "Construction",
                "limit_amount": 2_000_000,
                "drawn_balance": 1_500_000,
            },
        ],
    )

    sibling_pd = root / "facility_pd_final_combined.csv"
    _write_csv(
        sibling_pd,
        [
            {
                "facility_id": "FAC-1",
                "pd_model_stream": "cashflow",
                "score": 650,
                "score_band": "C",
                "risk_grade": "RG3",
                "pd_final": 0.04,
                "default_horizon_months": 12,
                "pd_model_name": "cashflow_logistic_scorecard",
                "pd_model_version": "v1.0",
                "as_of_date": "2026-04-09",
            },
            {
                "facility_id": "FAC-2",
                "pd_model_stream": "property",
                "score": 720,
                "score_band": "B",
                "risk_grade": "RG2",
                "pd_final": 0.02,
                "default_horizon_months": 12,
                "pd_model_name": "property_logistic_scorecard",
                "pd_model_version": "v1.0",
                "as_of_date": "2026-04-09",
            },
        ],
    )

    sibling_lgd = root / "lgd_final.csv"
    _write_csv(
        sibling_lgd,
        [
            {
                "facility_id": "FAC-1",
                "conduct_classification": "Green",
                "lgd_base": 0.42,
                "lgd_adj_lvr": 0.0,
                "lgd_adj_stage": 0.0,
                "lgd_adj_industry": 0.01,
                "lgd_adj_pd_band": 0.0,
                "lgd_adj_dscr": 0.0,
                "lgd_adj_conduct": 0.0,
                "lgd_adjusted": 0.43,
                "downturn_scalar": 1.1,
                "lgd_downturn": 0.473,
                "lgd_final": 0.473,
            },
            {
                "facility_id": "FAC-2",
                "conduct_classification": "Green",
                "lgd_base": 0.28,
                "lgd_adj_lvr": 0.0,
                "lgd_adj_stage": 0.0,
                "lgd_adj_industry": 0.01,
                "lgd_adj_pd_band": 0.0,
                "lgd_adj_dscr": 0.0,
                "lgd_adj_conduct": 0.0,
                "lgd_adjusted": 0.29,
                "downturn_scalar": 1.08,
                "lgd_downturn": 0.3132,
                "lgd_final": 0.3132,
            },
        ],
    )

    sibling_ead = root / "ead_by_facility.csv"
    _write_csv(
        sibling_ead,
        [
            {
                "facility_id": "FAC-1",
                "limit": 1_000_000,
                "drawn": 800_000,
                "undrawn": 200_000,
                "utilisation": 0.8,
                "base_ccf": 0.3,
                "downturn_ccf": 0.4,
                "ead_central": 860_000,
                "ead_downturn": 880_000,
            }
        ],
    )

    industry_scores = root / "industry_risk_score_table.csv"
    _write_csv(
        industry_scores,
        [
            {"industry": "Manufacturing", "industry_risk_score": 4.1},
            {"industry": "Construction", "industry_risk_score": 5.0},
        ],
    )

    monkeypatch.setattr(
        data_loader,
        "SIBLING_INPUT_CANDIDATES",
        {
            "pd_final": (sibling_pd,),
            "lgd_final": (sibling_lgd,),
            "ead_final": (sibling_ead,),
            "industry_scores": (industry_scores,),
            "industry_downturns": (),
        },
    )

    result = data_loader.load_input_tables(input_dir=input_dir, prefer_sibling_inputs=True)

    assert result["selected_input_strategy"] == "sibling_bundle"
    assert len(result["portfolio"]) == 2
    assert len(result["pd_final"]) == 2
    assert len(result["lgd_final"]) == 2
    assert result["ead_final"] is not None
    assert set(result["portfolio"]["facility_id"]) == {"FAC-1", "FAC-2"}


def test_load_input_tables_falls_back_to_demo_when_siblings_do_not_align(monkeypatch) -> None:
    root = Path("tests") / "_artifacts_loader" / "misaligned"
    input_dir = root / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(
        input_dir / "portfolio_input.csv",
        [
            {
                "facility_id": "FAC-1",
                "borrower_id": "BOR-1",
                "product_type": "SME Cash Flow Term Loan",
                "industry": "Manufacturing",
                "limit_amount": 1_000_000,
                "drawn_balance": 800_000,
            }
        ],
    )

    sibling_pd = root / "facility_pd_final_combined.csv"
    _write_csv(
        sibling_pd,
        [
            {
                "facility_id": "OTHER-1",
                "pd_model_stream": "cashflow",
                "score": 650,
                "score_band": "C",
                "risk_grade": "RG3",
                "pd_final": 0.04,
                "default_horizon_months": 12,
                "pd_model_name": "cashflow_logistic_scorecard",
                "pd_model_version": "v1.0",
                "as_of_date": "2026-04-09",
            }
        ],
    )

    sibling_lgd = root / "lgd_final.csv"
    _write_csv(
        sibling_lgd,
        [
            {
                "facility_id": "OTHER-1",
                "conduct_classification": "Green",
                "lgd_base": 0.42,
                "lgd_adj_lvr": 0.0,
                "lgd_adj_stage": 0.0,
                "lgd_adj_industry": 0.01,
                "lgd_adj_pd_band": 0.0,
                "lgd_adj_dscr": 0.0,
                "lgd_adj_conduct": 0.0,
                "lgd_adjusted": 0.43,
                "downturn_scalar": 1.1,
                "lgd_downturn": 0.473,
                "lgd_final": 0.473,
            }
        ],
    )

    monkeypatch.setattr(
        data_loader,
        "SIBLING_INPUT_CANDIDATES",
        {
            "pd_final": (sibling_pd,),
            "lgd_final": (sibling_lgd,),
            "ead_final": (),
            "industry_scores": (),
            "industry_downturns": (),
        },
    )

    result = data_loader.load_input_tables(input_dir=input_dir, prefer_sibling_inputs=True)

    assert result["selected_input_strategy"] == "demo_generated"
    rejected = result["input_source_report"][
        (result["input_source_report"]["strategy"] == "sibling_bundle")
        & (result["input_source_report"]["status"] == "rejected")
    ]
    assert not rejected.empty
    assert len(result["portfolio"]) > 1
