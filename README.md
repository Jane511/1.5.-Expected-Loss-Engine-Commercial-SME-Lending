# Commercial Expected Loss Engine Project

This repository is the expected loss integration layer in the commercial credit-risk stack. It combines upstream PD, LGD, and EAD outputs, with optional industry reference data, to produce loan-level expected loss measures, portfolio summaries, IFRS 9 style tables, and downstream pricing or stress inputs. The project is positioned as the bridge between component risk models and downstream portfolio analytics.

## What this repo is

This project demonstrates how separate commercial credit-risk components can be brought together into one practical expected loss workflow. It is built for portfolio review and recruiter inspection, so the data is synthetic, the logic is transparent, and the outputs are shaped for downstream reuse rather than for a black-box model presentation.

## Where it sits in the stack

Upstream inputs:
- `PD-and-scorecard-commercial`
- `LGD-commercial`
- `EAD-CCF-commercial`
- optional reference context from `industry-analysis`

Downstream consumers:
- `stress-testing-commercial`
- `RAROC-pricing-and-return-hurdle`
- `portfolio-monitor-commercial` (planned downstream repo; not yet published on the public portfolio)
- `RWA-capital-commercial`

Some downstream modules are planned but not yet published on the public portfolio.

## Key outputs

- `outputs/tables/loan_level_el.csv`
- `outputs/tables/segment_expected_loss_summary.csv`
- `outputs/tables/portfolio_summary.csv`
- `outputs/tables/pricing_table.csv`
- `outputs/tables/stress_test_results.csv`
- `outputs/tables/ifrs9_ecl_by_facility.csv`
- `outputs/tables/concentration_summary.csv`
- `outputs/tables/input_source_report.csv`
- `outputs/tables/pipeline_validation_report.csv`

## Repo structure

- `data/`: staged input bundles, processed working tables, and external reference files
- `src/`: reusable expected loss, pricing, staging, and pipeline logic
- `docs/`: methodology, assumptions, pricing logic, stress notes, and validation material
- `notebooks/`: reviewer-facing walkthrough notebooks
- `outputs/`: exported tables, charts, reports, and sample artifacts
- `tests/`: validation and regression checks

## How to run

```powershell
python -m src.pipeline
```

Compatibility alias:

```powershell
python -m src.codex_run_pipeline
```

## Limitations / Demo-Only Note

- All inputs are synthetic or public-style demo data.
- The integration logic is designed to be explainable and reusable rather than to mirror a production impairment engine exactly.
- Downstream tables are intentionally flat and portable so the repo can demonstrate stack integration without relying on workspace-specific conventions.
- Some downstream modules are planned but not yet published on the public portfolio.
