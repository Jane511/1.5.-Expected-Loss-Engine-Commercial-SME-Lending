from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR = ROOT / "outputs" / "tables"
CHART_DIR = ROOT / "outputs" / "charts"
DOCS_DIR = ROOT / "docs"
NOTEBOOKS_DIR = ROOT / "notebooks"

DEFAULT_INPUT_FILES = {
    "portfolio": INPUT_DIR / "portfolio_input.csv",
    "pd_final": INPUT_DIR / "facility_pd_final_combined.csv",
    "lgd_final": INPUT_DIR / "lgd_final.csv",
    "downturn_overlays": INPUT_DIR / "downturn_overlays.csv",
}

DEFAULT_OUTPUT_FILES = {
    "loan_level_el": OUTPUT_DIR / "loan_level_el.csv",
    "segment_summary": OUTPUT_DIR / "segment_expected_loss_summary.csv",
    "portfolio_summary": OUTPUT_DIR / "portfolio_summary.csv",
    "pricing_table": OUTPUT_DIR / "pricing_table.csv",
    "stress_results": OUTPUT_DIR / "stress_test_results.csv",
}

SIBLING_INPUT_CANDIDATES = {
    "portfolio_sample": (
        ROOT.parent / "PD and score card commercial" / "outputs" / "samples" / "demo_input.csv",
        ROOT.parent / "EAD CCF commercial" / "outputs" / "samples" / "demo_input.csv",
        ROOT.parent / "LG commercial" / "outputs" / "samples" / "demo_input.csv",
    ),
    "pd_final": (
        ROOT.parent / "PD and score card commercial" / "outputs" / "tables" / "pd_model_output.csv",
        ROOT.parent / "PD and score card commercial" / "output" / "pd_final" / "facility_pd_final_combined.csv",
        ROOT.parent / "1.2 PD and Score Card_Cashflow & Property backed Lending" / "output" / "pd_final" / "facility_pd_final_combined.csv",
    ),
    "lgd_final": (
        ROOT.parent / "LG commercial" / "outputs" / "tables" / "downturn_lgd_output.csv",
        ROOT.parent / "LG commercial" / "outputs" / "tables" / "lgd_final.csv",
        ROOT.parent / "2. APRA LGD Model" / "outputs" / "tables" / "lgd_final.csv",
    ),
    "ead_final": (
        ROOT.parent / "EAD CCF commercial" / "outputs" / "tables" / "ead_by_facility.csv",
    ),
    "industry_scores": (
        ROOT.parent / "industry risk analysis" / "outputs" / "tables" / "industry_risk_score_table.csv",
    ),
    "industry_downturns": (
        ROOT.parent / "industry risk analysis" / "outputs" / "tables" / "downturn_overlay_table.csv",
    ),
    "industry_stress_matrix": (
        ROOT.parent / "9.Industry Risk Analysis_Australia" / "output" / "tables" / "industry_stress_test_matrix.csv",
    ),
}

AS_OF_DATE = "2026-04-09"
RANDOM_SEED = 42
N_FACILITIES = 180

PRODUCT_SETTINGS = {
    "SME Cash Flow Term Loan": {
        "loan_type": "term",
        "limit_range": (250_000, 2_500_000),
        "drawn_range": (0.72, 1.00),
        "term_options": (24, 36, 48),
        "interest_base": 0.082,
        "risk_factor": 0.18,
        "security_types": (
            "General Security Agreement",
            "Receivables Security",
            "Unsecured",
        ),
        "security_weights": (0.55, 0.20, 0.25),
    },
    "Property Backed Loan": {
        "loan_type": "term",
        "limit_range": (500_000, 6_000_000),
        "drawn_range": (0.80, 1.00),
        "term_options": (36, 48, 60),
        "interest_base": 0.071,
        "risk_factor": 0.12,
        "security_types": (
            "Commercial Property",
            "Residential Investment Property",
        ),
        "security_weights": (0.65, 0.35),
    },
    "Overdraft / Revolving Working Capital": {
        "loan_type": "revolving",
        "limit_range": (100_000, 1_500_000),
        "drawn_range": (0.35, 0.80),
        "term_options": (12, 12, 24),
        "interest_base": 0.094,
        "risk_factor": 0.24,
        "security_types": (
            "General Security Agreement",
            "Unsecured",
        ),
        "security_weights": (0.75, 0.25),
    },
}

INDUSTRY_SETTINGS = {
    "Agriculture, Forestry and Fishing": {"risk_factor": 0.20, "revenue_range": (3_000_000, 18_000_000), "ebitda_margin_range": (0.10, 0.18)},
    "Manufacturing": {"risk_factor": 0.22, "revenue_range": (5_000_000, 35_000_000), "ebitda_margin_range": (0.08, 0.16)},
    "Retail Trade": {"risk_factor": 0.18, "revenue_range": (2_000_000, 24_000_000), "ebitda_margin_range": (0.05, 0.11)},
    "Wholesale Trade": {"risk_factor": 0.15, "revenue_range": (4_000_000, 30_000_000), "ebitda_margin_range": (0.06, 0.13)},
    "Accommodation and Food Services": {"risk_factor": 0.24, "revenue_range": (1_500_000, 15_000_000), "ebitda_margin_range": (0.07, 0.15)},
    "Construction": {"risk_factor": 0.21, "revenue_range": (4_000_000, 40_000_000), "ebitda_margin_range": (0.06, 0.14)},
    "Health Care and Social Assistance": {"risk_factor": 0.10, "revenue_range": (3_000_000, 30_000_000), "ebitda_margin_range": (0.12, 0.22)},
    "Professional, Scientific and Technical Services": {"risk_factor": 0.11, "revenue_range": (2_000_000, 20_000_000), "ebitda_margin_range": (0.15, 0.28)},
    "Transport, Postal and Warehousing": {"risk_factor": 0.17, "revenue_range": (4_000_000, 32_000_000), "ebitda_margin_range": (0.08, 0.16)},
}

REGION_FACTORS = {
    "NSW": 0.00,
    "VIC": 0.02,
    "QLD": 0.03,
    "WA": -0.01,
    "SA": 0.01,
}

RISK_GRADE_PD_LOOKUP = {
    "RG1": 0.012,
    "RG2": 0.022,
    "RG3": 0.041,
    "RG4": 0.079,
    "RG5": 0.150,
}

RISK_GRADE_TO_SCORE_BAND = {
    "RG1": "A",
    "RG2": "B",
    "RG3": "C",
    "RG4": "D",
    "RG5": "E",
}

CCF_LOOKUP = {
    "strong": 0.30,
    "average": 0.50,
    "weak": 0.75,
}

PRICING_ASSUMPTIONS = {
    "funding_cost": 0.018,
    "operating_cost": 0.007,
    "target_return": 0.025,
}

STRESS_SCENARIOS = (
    {"scenario": "base", "pd_multiplier": 1.00, "lgd_multiplier": 1.00, "ccf_multiplier": 1.00, "scenario_description": "Base conditions"},
    {"scenario": "mild", "pd_multiplier": 1.25, "lgd_multiplier": 1.10, "ccf_multiplier": 1.10, "scenario_description": "Moderate deterioration in credit conditions"},
    {"scenario": "severe", "pd_multiplier": 2.00, "lgd_multiplier": 1.30, "ccf_multiplier": 1.20, "scenario_description": "Sharp downturn with weaker recoveries and higher revolver usage"},
)
