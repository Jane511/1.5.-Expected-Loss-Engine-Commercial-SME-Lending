# Methodology - Expected-Loss-Engine-Australia

1. Attempt to reconcile the current sibling PD/LGD/EAD exports to their shared facility universe.
2. Use the local `portfolio_input.csv` when it aligns to that universe; otherwise use an aligned sibling sample portfolio.
3. Fall back to the local staged input bundle when sibling exports are unavailable or not aligned.
4. Generate demo inputs only when no coherent bundle is available or when the demo refresh flag is requested.
5. Build the facility-level expected-loss dataset and attach optional external EAD and industry context.
6. Compute IFRS 9 staging/ECL, pricing, stress, and concentration outputs.
7. Generate the chart pack for EL waterfall, risk-grade distribution, concentration heatmap, and stress comparison.
8. Validate the run and export CSV outputs plus input-source diagnostics.

## Output contract

- `outputs/tables/loan_level_el.csv`
- `outputs/tables/segment_expected_loss_summary.csv`
- `outputs/tables/portfolio_summary.csv`
- `outputs/tables/pricing_table.csv`
- `outputs/tables/stress_test_results.csv`
- `outputs/tables/ifrs9_ecl_by_facility.csv`
- `outputs/tables/ifrs9_stage_summary.csv`
- `outputs/tables/concentration_by_sector.csv`
- `outputs/tables/concentration_by_region.csv`
- `outputs/tables/concentration_top_borrowers.csv`
- `outputs/tables/concentration_summary.csv`
- `outputs/tables/input_source_report.csv`
- `outputs/tables/pipeline_validation_report.csv`
