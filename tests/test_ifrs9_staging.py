from __future__ import annotations

import pandas as pd
import pytest

from src.ifrs9_staging import allocate_stage, compute_ecl, summarise_by_stage


@pytest.fixture()
def sample_portfolio():
    return pd.DataFrame([
        {"facility_id": "F001", "borrower_id": "B001", "pd_final": 0.015, "lgd_final": 0.40,
         "ead": 1_000_000, "risk_grade": "RG1", "arrears_days": 0, "watchlist_flag": False,
         "loan_term_months": 36},
        {"facility_id": "F002", "borrower_id": "B002", "pd_final": 0.045, "lgd_final": 0.55,
         "ead": 800_000, "risk_grade": "RG3", "arrears_days": 35, "watchlist_flag": False,
         "loan_term_months": 24},
        {"facility_id": "F003", "borrower_id": "B003", "pd_final": 0.12, "lgd_final": 0.65,
         "ead": 600_000, "risk_grade": "RG5", "arrears_days": 95, "watchlist_flag": True,
         "loan_term_months": 48},
        {"facility_id": "F004", "borrower_id": "B004", "pd_final": 0.03, "lgd_final": 0.42,
         "ead": 1_200_000, "risk_grade": "RG2", "arrears_days": 0, "watchlist_flag": True,
         "loan_term_months": 36},
    ])


def test_stage_allocation_performing(sample_portfolio):
    result = allocate_stage(sample_portfolio)
    f001 = result[result["facility_id"] == "F001"].iloc[0]
    assert f001["ifrs9_stage"] == 1


def test_stage_allocation_arrears_triggers_stage2(sample_portfolio):
    result = allocate_stage(sample_portfolio)
    f002 = result[result["facility_id"] == "F002"].iloc[0]
    assert f002["ifrs9_stage"] == 2


def test_stage_allocation_default_triggers_stage3(sample_portfolio):
    result = allocate_stage(sample_portfolio)
    f003 = result[result["facility_id"] == "F003"].iloc[0]
    assert f003["ifrs9_stage"] == 3


def test_watchlist_triggers_stage2(sample_portfolio):
    result = allocate_stage(sample_portfolio)
    f004 = result[result["facility_id"] == "F004"].iloc[0]
    assert f004["ifrs9_stage"] == 2


def test_ecl_computation_stage1_uses_12m_pd(sample_portfolio):
    staged = allocate_stage(sample_portfolio)
    result = compute_ecl(staged)
    f001 = result[result["facility_id"] == "F001"].iloc[0]
    expected = round(0.015 * 0.40 * 1_000_000, 2)
    assert f001["ecl_provision"] == expected


def test_ecl_computation_stage3_uses_lgd_times_ead(sample_portfolio):
    staged = allocate_stage(sample_portfolio)
    result = compute_ecl(staged)
    f003 = result[result["facility_id"] == "F003"].iloc[0]
    expected = round(0.65 * 600_000, 2)
    assert f003["ecl_provision"] == expected


def test_stage_summary_has_all_stages_plus_total(sample_portfolio):
    staged = allocate_stage(sample_portfolio)
    ecl = compute_ecl(staged)
    summary = summarise_by_stage(ecl)
    stages_present = set(summary["ifrs9_stage"].tolist())
    assert 0 in stages_present  # total row
    assert summary["share_of_portfolio_ecl"].iloc[-1] == 1.0


def test_ecl_coverage_ratio_positive(sample_portfolio):
    staged = allocate_stage(sample_portfolio)
    result = compute_ecl(staged)
    assert (result["ecl_coverage_ratio"] >= 0).all()
    assert (result["ecl_coverage_ratio"] <= 1).all()
