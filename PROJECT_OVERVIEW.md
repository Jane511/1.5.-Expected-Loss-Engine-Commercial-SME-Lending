# Project Overview - expected-loss-engine-commercial

`expected-loss-engine-commercial` is the middle-to-downstream integration layer that combines PD, LGD, and EAD into expected loss outputs for the commercial credit-risk portfolio.

## Portfolio role

`expected-loss-engine-commercial` is the expected-loss integration engine that combines PD, LGD, and EAD into facility- and portfolio-level loss outputs.

## Upstream inputs

- `PD-and-scorecard-commercial`
- `LGD-commercial`
- `EAD-CCF-commercial`

Optional supporting reference inputs:
- `industry-analysis`

## Downstream consumers

- `stress-testing-commercial`
- `RAROC-pricing-and-return-hurdle`
- `portfolio-monitor-commercial`
- `RWA-capital-commercial`

## Rebuilt deliverables

- Standard repo structure with `data`, `docs`, `notebooks`, `src`, `scripts`, `outputs`, and `tests`.
- End-to-end Codex demo pipeline: `python -m src.codex_run_pipeline`.
- Required output contract files in `outputs/tables`.
