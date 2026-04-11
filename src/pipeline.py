from __future__ import annotations

import argparse
from pathlib import Path

from .aggregation import summarise_portfolio, summarise_segment_expected_loss
from .charts import generate_all_charts
from .concentration import concentration_summary, sector_concentration, region_concentration, single_name_concentration
from .config import DEFAULT_OUTPUT_FILES, INPUT_DIR, OUTPUT_DIR, PROCESSED_DIR
from .data_loader import load_input_tables
from .expected_loss import build_expected_loss_dataset
from .ifrs9_staging import allocate_stage, compute_ecl, summarise_by_stage
from .pricing import apply_pricing, summarise_pricing
from .stress_testing import run_stress_tests
from .utils import ensure_directories, save_dataframe
from .validation import validate_pipeline_results


def run_pipeline(
    input_dir: str | Path | None = None,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    refresh_demo_inputs: bool = False,
    prefer_sibling_inputs: bool = True,
    strict_sibling_inputs: bool = False,
    persist: bool = True,
) -> dict:
    input_path = Path(input_dir) if input_dir is not None else INPUT_DIR
    processed_path = Path(processed_dir) if processed_dir is not None else PROCESSED_DIR
    output_path = Path(output_dir) if output_dir is not None else OUTPUT_DIR
    ensure_directories(input_path, processed_path, output_path)

    inputs = load_input_tables(
        input_dir=input_path,
        refresh_demo=refresh_demo_inputs,
        prefer_sibling_inputs=prefer_sibling_inputs,
        strict_sibling_inputs=strict_sibling_inputs,
    )
    expected_loss_df = build_expected_loss_dataset(
        inputs["portfolio"],
        inputs["pd_final"],
        inputs["lgd_final"],
        ead_df=inputs.get("ead_final"),
        industry_scores_df=inputs.get("industry_scores"),
        industry_downturn_df=inputs.get("industry_downturns"),
    )
    segment_summary = summarise_segment_expected_loss(expected_loss_df)
    portfolio_summary = summarise_portfolio(expected_loss_df)
    pricing_df = apply_pricing(expected_loss_df)
    pricing_summary = summarise_pricing(pricing_df)
    stress_summary = run_stress_tests(expected_loss_df, inputs["downturn_overlays"])

    ifrs9_df = allocate_stage(expected_loss_df)
    ifrs9_df = compute_ecl(ifrs9_df)
    ifrs9_stage_summary = summarise_by_stage(ifrs9_df)

    sector_conc = sector_concentration(expected_loss_df)
    region_conc = region_concentration(expected_loss_df)
    borrower_conc = single_name_concentration(expected_loss_df)
    conc_summary = concentration_summary(expected_loss_df)

    result = {
        "inputs": inputs,
        "selected_input_strategy": inputs.get("selected_input_strategy"),
        "input_source_report": inputs.get("input_source_report"),
        "loan_level_el": expected_loss_df,
        "segment_summary": segment_summary,
        "portfolio_summary": portfolio_summary,
        "pricing_table": pricing_summary,
        "stress_results": stress_summary,
        "ifrs9_el": ifrs9_df,
        "ifrs9_stage_summary": ifrs9_stage_summary,
        "sector_concentration": sector_conc,
        "region_concentration": region_conc,
        "borrower_concentration": borrower_conc,
        "concentration_summary": conc_summary,
    }
    validation_report = validate_pipeline_results(result)
    result["validation_report"] = validation_report

    if persist:
        save_dataframe(expected_loss_df, output_path / DEFAULT_OUTPUT_FILES["loan_level_el"].name)
        save_dataframe(segment_summary, output_path / DEFAULT_OUTPUT_FILES["segment_summary"].name)
        save_dataframe(portfolio_summary, output_path / DEFAULT_OUTPUT_FILES["portfolio_summary"].name)
        save_dataframe(pricing_summary, output_path / DEFAULT_OUTPUT_FILES["pricing_table"].name)
        save_dataframe(stress_summary, output_path / DEFAULT_OUTPUT_FILES["stress_results"].name)
        save_dataframe(expected_loss_df, processed_path / "expected_loss_dataset.csv")
        save_dataframe(pricing_df, processed_path / "pricing_dataset.csv")
        save_dataframe(ifrs9_df, output_path / "ifrs9_ecl_by_facility.csv")
        save_dataframe(ifrs9_stage_summary, output_path / "ifrs9_stage_summary.csv")
        save_dataframe(sector_conc, output_path / "concentration_by_sector.csv")
        save_dataframe(region_conc, output_path / "concentration_by_region.csv")
        save_dataframe(borrower_conc, output_path / "concentration_top_borrowers.csv")
        save_dataframe(conc_summary, output_path / "concentration_summary.csv")
        if result["input_source_report"] is not None:
            save_dataframe(result["input_source_report"], output_path / "input_source_report.csv")
        save_dataframe(validation_report, output_path / "pipeline_validation_report.csv")
        chart_dir = output_path.parent / "charts" if output_path.name == "tables" else output_path / "charts"
        generate_all_charts(result, chart_dir=chart_dir)

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Expected Loss Engine pipeline.")
    parser.add_argument(
        "--refresh-demo-inputs",
        action="store_true",
        help="Regenerate the synthetic input files before running the pipeline.",
    )
    parser.add_argument(
        "--strict-sibling-inputs",
        action="store_true",
        help="Fail if sibling repo exports cannot be reconciled to the local portfolio input.",
    )
    args = parser.parse_args()

    result = run_pipeline(
        refresh_demo_inputs=args.refresh_demo_inputs,
        strict_sibling_inputs=args.strict_sibling_inputs,
        persist=True,
    )
    total_ead = float(result["loan_level_el"]["ead"].sum())
    total_el = float(result["loan_level_el"]["expected_loss"].sum())
    total_ecl = float(result["ifrs9_el"]["ecl_provision"].sum())
    stage_counts = result["ifrs9_el"]["ifrs9_stage"].value_counts().sort_index()
    print(f"Facilities processed: {len(result['loan_level_el'])}")
    print(f"Portfolio EAD: {total_ead:,.2f}")
    print(f"Portfolio expected loss: {total_el:,.2f}")
    print(f"IFRS 9 ECL provision: {total_ecl:,.2f}")
    print(f"Stage allocation: {', '.join(f'Stage {k}: {v}' for k, v in stage_counts.items())}")
    print(f"Concentration dimensions checked: {len(result['concentration_summary'])}")
    print(f"Output files written to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
