from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.charts import (
    generate_all_charts,
    plot_concentration_heatmap,
    plot_el_waterfall,
    plot_el_by_risk_grade,
    plot_el_by_product,
    plot_ifrs9_stage_breakdown,
    plot_stress_test_comparison,
)


@pytest.fixture()
def el_data():
    return pd.DataFrame([
        {"facility_id": "F001", "risk_grade": "RG1", "product_type": "Term Loan",
         "ead": 1_000_000, "expected_loss": 6_000},
        {"facility_id": "F002", "risk_grade": "RG3", "product_type": "Overdraft",
         "ead": 800_000, "expected_loss": 18_000},
        {"facility_id": "F003", "risk_grade": "RG5", "product_type": "Term Loan",
         "ead": 600_000, "expected_loss": 45_000},
    ])


@pytest.fixture()
def stage_summary():
    return pd.DataFrame([
        {"ifrs9_stage": 1, "stage_label": "Performing", "total_ead": 1_800_000,
         "total_ecl_provision": 24_000},
        {"ifrs9_stage": 2, "stage_label": "Under-performing (SICR)", "total_ead": 800_000,
         "total_ecl_provision": 18_000},
        {"ifrs9_stage": 3, "stage_label": "Credit-impaired", "total_ead": 600_000,
         "total_ecl_provision": 390_000},
        {"ifrs9_stage": 0, "stage_label": "Total Portfolio", "total_ead": 3_200_000,
         "total_ecl_provision": 432_000},
    ])


@pytest.fixture()
def sector_data():
    return pd.DataFrame([
        {"industry": "Manufacturing", "ead_share": 0.42, "el_share": 0.38, "total_ead": 2_100_000},
        {"industry": "Retail Trade", "ead_share": 0.33, "el_share": 0.35, "total_ead": 1_650_000},
        {"industry": "Construction", "ead_share": 0.25, "el_share": 0.27, "total_ead": 1_250_000},
    ])


@pytest.fixture()
def stress_data():
    return pd.DataFrame([
        {"scenario": "base", "total_el": 100_000},
        {"scenario": "mild", "total_el": 135_000},
        {"scenario": "severe", "total_el": 210_000},
    ])


@pytest.fixture()
def chart_dir(request):
    path = Path("tests") / "_artifacts" / "charts" / request.node.name
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_el_by_risk_grade_creates_file(el_data, chart_dir):
    path = plot_el_by_risk_grade(el_data, chart_dir=chart_dir)
    assert path.exists()
    assert path.suffix == ".png"


def test_el_waterfall_creates_file(el_data, chart_dir):
    path = plot_el_waterfall(el_data, chart_dir=chart_dir)
    assert path.exists()
    assert path.name == "el_waterfall.png"


def test_el_by_product_creates_file(el_data, chart_dir):
    path = plot_el_by_product(el_data, chart_dir=chart_dir)
    assert path.exists()


def test_ifrs9_stage_breakdown_creates_file(stage_summary, chart_dir):
    path = plot_ifrs9_stage_breakdown(stage_summary, chart_dir=chart_dir)
    assert path.exists()


def test_concentration_heatmap_creates_file(sector_data, chart_dir):
    path = plot_concentration_heatmap(sector_data, chart_dir=chart_dir)
    assert path.exists()


def test_stress_test_comparison_creates_file(stress_data, chart_dir):
    path = plot_stress_test_comparison(stress_data, chart_dir=chart_dir)
    assert path.exists()


def test_generate_all_charts(el_data, stage_summary, sector_data, stress_data, chart_dir):
    results = {
        "loan_level_el": el_data,
        "ifrs9_stage_summary": stage_summary,
        "sector_concentration": sector_data,
        "stress_results": stress_data,
    }
    paths = generate_all_charts(results, chart_dir=chart_dir)
    names = {path.name for path in paths}
    assert "el_waterfall.png" in names
    assert "el_by_risk_grade.png" in names
    assert "ifrs9_stage_breakdown.png" in names
    assert "concentration_heatmap.png" in names
    assert "stress_test_comparison.png" in names
    assert all(p.exists() for p in paths)
