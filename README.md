# Expected-Loss-Engine-Australia

## What this repo is

This repo is the downstream integration engine for expected loss for a bank-style Australian credit-risk portfolio demonstration. It uses public-data friendly and synthetic sample data only.

## Where it sits in the full credit-risk stack

Upstream inputs:
- PD-and-scorecard-commercial
- LGD-commercial
- EAD-CCF-commercial
- industry-analysis

Downstream consumers:
- stress-testing-commercial
- RAROC-pricing-and-return-hurdle
- planned portfolio-monitoring consumer
- RWA-capital-commercial

## Inputs

The canonical pipeline first tries to reconcile the current sibling-repo exports on their shared facility universe. It uses a local `data/input/portfolio_input.csv` when that file aligns, otherwise it falls back to an aligned sibling sample portfolio. The local staged bundle under `data/input/` is used only when no coherent sibling bundle is available. Demo inputs are generated only when no coherent local or sibling bundle is available, or when `--refresh-demo-inputs` is used.

## What the pipeline does

It loads the selected input bundle, assembles a facility-level EL dataset, attaches optional external EAD and industry context, computes IFRS 9 staging/ECL, pricing, stress, and concentration outputs, validates the run, and writes downstream-friendly CSV files plus a chart pack under `outputs/charts/` including the EL waterfall, risk-grade distribution, concentration heatmap, and stress comparison.

## Outputs

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

## How to run

```powershell
python -m src.pipeline
```

Compatibility alias:

```powershell
python -m src.codex_run_pipeline
```

## Limitations and synthetic-data note

- Demo data is synthetic and not confidential bank data.
- Thresholds, overlays, and formulae are transparent portfolio-demonstration assumptions.
- The sibling-first path depends on the current downstream-friendly exports in the upstream repos. If those are replaced with incompatible legacy files, the loader will reject them and record the reason in `input_source_report.csv`.
- Production use would require governed source data, calibration, model validation, and approval.

## How it connects to the next repo

The exported CSV files are intentionally flat and can be copied to the next repository's `data/external` or replaced with validated production extracts.
