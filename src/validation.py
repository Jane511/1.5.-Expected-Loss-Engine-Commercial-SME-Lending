from __future__ import annotations

import pandas as pd


def validate_pipeline_results(result: dict) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    required_frames = {
        "loan_level_el": "Loan-level expected loss dataset",
        "segment_summary": "Segment summary",
        "portfolio_summary": "Portfolio summary",
        "pricing_table": "Pricing summary",
        "stress_results": "Stress-testing summary",
        "ifrs9_el": "IFRS 9 facility table",
        "ifrs9_stage_summary": "IFRS 9 stage summary",
        "concentration_summary": "Concentration summary",
    }
    for key, label in required_frames.items():
        frame = result.get(key)
        rows.append(
            {
                "check_name": f"required_frame::{key}",
                "status": bool(frame is not None and not frame.empty),
                "detail": f"{label} present and non-empty.",
            }
        )

    loan_level = result.get("loan_level_el")
    if loan_level is not None and not loan_level.empty:
        rows.extend(
            [
                {
                    "check_name": "expected_loss_non_negative",
                    "status": bool((loan_level["expected_loss"] >= 0).all()),
                    "detail": "Expected loss is non-negative for every facility.",
                },
                {
                    "check_name": "pd_final_between_zero_and_one",
                    "status": bool(loan_level["pd_final"].between(0, 1).all()),
                    "detail": "PD values are within [0, 1].",
                },
                {
                    "check_name": "lgd_final_between_zero_and_one",
                    "status": bool(loan_level["lgd_final"].between(0, 1).all()),
                    "detail": "LGD values are within [0, 1].",
                },
                {
                    "check_name": "ead_non_negative",
                    "status": bool((loan_level["ead"] >= 0).all()),
                    "detail": "EAD is non-negative for every facility.",
                },
            ]
        )

    ifrs9 = result.get("ifrs9_el")
    if ifrs9 is not None and not ifrs9.empty:
        stages = set(ifrs9["ifrs9_stage"].unique().tolist())
        rows.append(
            {
                "check_name": "ifrs9_stage_values_valid",
                "status": stages.issubset({1, 2, 3}),
                "detail": f"Observed IFRS 9 stages: {sorted(stages)}",
            }
        )

    stress = result.get("stress_results")
    if stress is not None and not stress.empty:
        rows.append(
            {
                "check_name": "stress_has_base_mild_severe",
                "status": set(stress["scenario"]) == {"base", "mild", "severe"},
                "detail": "Stress scenarios include base, mild, and severe.",
            }
        )

    input_source_report = result.get("input_source_report")
    if input_source_report is not None and not input_source_report.empty:
        selected = input_source_report[
            (input_source_report["source_key"] == "strategy") & (input_source_report["status"] == "selected")
        ]
        rows.append(
            {
                "check_name": "input_strategy_selected",
                "status": bool(not selected.empty),
                "detail": (
                    ", ".join(selected["strategy"].tolist())
                    if not selected.empty
                    else "No selected input strategy row was recorded."
                ),
            }
        )

    return pd.DataFrame.from_records(rows)
