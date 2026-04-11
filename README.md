# expected-loss-engine-commercial

`expected-loss-engine-commercial` combines PD, LGD, and EAD inputs into facility-level and portfolio-level expected loss outputs for the commercial credit-risk stack.

## What this repo is

This repo is the downstream expected-loss integration layer for a bank-style Australian credit-risk portfolio demonstration. It uses public-data friendly and synthetic sample data only.

## Where it sits in the full credit-risk stack

Upstream inputs:
- `PD-and-scorecard-commercial`
- `LGD-commercial`
- `EAD-CCF-commercial`

Optional supporting reference inputs:
- `industry-analysis`

Downstream consumers:
- `stress-testing-commercial`
- `RAROC-pricing-and-return-hurdle`
- `Portfolio-Monitoring-MIS`
- `RWA-capital-commercial`

## Inputs

The canonical pipeline first tries to reconcile the current upstream repo exports from `PD-and-scorecard-commercial`, `LGD-commercial`, and `EAD-CCF-commercial` on their shared facility universe. It uses a local `data/input/portfolio_input.csv` when that file aligns, otherwise it falls back to an aligned upstream sample portfolio. The staged bundle under `data/input/` is used only when no coherent upstream bundle is available. Optional industry context can be added from `industry-analysis` when those reference tables are available. Demo inputs are generated only when no coherent local or upstream-aligned bundle is available, or when `--refresh-demo-inputs` is used.

## What the pipeline does

It loads the selected upstream bundle, combines PD, LGD, and EAD into a facility-level expected-loss dataset, attaches optional industry context, computes IFRS 9 staging/ECL, pricing, stress, and concentration outputs, validates the run, and writes downstream-friendly CSV files plus a chart pack under `outputs/charts/` including the EL waterfall, risk-grade distribution, concentration heatmap, and stress comparison.

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
- The upstream-export path depends on the current downstream-friendly exports in the upstream repos. If those are replaced with incompatible legacy files, the loader will reject them and record the reason in `input_source_report.csv`.
- Production use would require governed source data, calibration, model validation, and approval.

## How it connects to the next repo

The exported CSV files are intentionally flat and can be staged into downstream repos such as `../stress-testing-commercial/`, `../RAROC-pricing-and-return-hurdle/`, `../Portfolio-Monitoring-MIS/`, or `../RWA-capital-commercial/` via their `data/external/` folders, or replaced with validated production extracts.
