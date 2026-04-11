from __future__ import annotations

import pandas as pd
import pytest

from src.concentration import (
    concentration_summary,
    portfolio_hhi,
    region_concentration,
    sector_concentration,
    single_name_concentration,
)


@pytest.fixture()
def sample_portfolio():
    return pd.DataFrame([
        {"facility_id": "F001", "borrower_id": "B001", "industry": "Manufacturing",
         "region": "NSW", "ead": 1_000_000, "expected_loss": 15_000},
        {"facility_id": "F002", "borrower_id": "B001", "industry": "Manufacturing",
         "region": "NSW", "ead": 500_000, "expected_loss": 8_000},
        {"facility_id": "F003", "borrower_id": "B002", "industry": "Retail Trade",
         "region": "VIC", "ead": 800_000, "expected_loss": 20_000},
        {"facility_id": "F004", "borrower_id": "B003", "industry": "Construction",
         "region": "QLD", "ead": 700_000, "expected_loss": 12_000},
        {"facility_id": "F005", "borrower_id": "B004", "industry": "Manufacturing",
         "region": "NSW", "ead": 600_000, "expected_loss": 9_000},
    ])


def test_single_name_returns_sorted_by_ead(sample_portfolio):
    result = single_name_concentration(sample_portfolio)
    assert result.iloc[0]["borrower_id"] == "B001"
    assert result.iloc[0]["total_ead"] == 1_500_000


def test_single_name_portfolio_shares_sum_close_to_one(sample_portfolio):
    result = single_name_concentration(sample_portfolio, top_n=100)
    assert abs(result["portfolio_share"].sum() - 1.0) < 0.001


def test_single_name_breach_flag(sample_portfolio):
    result = single_name_concentration(sample_portfolio, limit_pct=0.30)
    b001 = result[result["borrower_id"] == "B001"].iloc[0]
    assert b001["breach_flag"] is True or b001["breach_flag"] == True


def test_sector_concentration_covers_all_industries(sample_portfolio):
    result = sector_concentration(sample_portfolio)
    assert set(result["industry"]) == {"Manufacturing", "Retail Trade", "Construction"}


def test_sector_ead_shares_sum_to_one(sample_portfolio):
    result = sector_concentration(sample_portfolio)
    assert abs(result["ead_share"].sum() - 1.0) < 0.001


def test_region_concentration(sample_portfolio):
    result = region_concentration(sample_portfolio)
    nsw = result[result["region"] == "NSW"].iloc[0]
    assert nsw["facility_count"] == 3


def test_hhi_perfectly_concentrated():
    df = pd.DataFrame([
        {"facility_id": "F1", "industry": "A", "ead": 1_000_000},
    ])
    result = portfolio_hhi(df, "industry")
    assert result["hhi"] == 1.0
    assert result["status"] == "elevated"


def test_hhi_diversified():
    df = pd.DataFrame([
        {"facility_id": f"F{i}", "industry": f"Ind{i}", "ead": 100_000}
        for i in range(20)
    ])
    result = portfolio_hhi(df, "industry")
    assert result["hhi"] == 0.05
    assert result["status"] == "within_appetite"


def test_concentration_summary_has_all_dimensions(sample_portfolio):
    result = concentration_summary(sample_portfolio)
    dims = set(result["dimension"])
    assert "industry" in dims
    assert "region" in dims
    assert "borrower_id" in dims
