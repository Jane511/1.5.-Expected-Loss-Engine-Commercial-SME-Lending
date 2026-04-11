# Project Overview - expected-loss-engine-commercial

`expected-loss-engine-commercial` is the middle-to-downstream integration layer that combines PD, LGD, and EAD into expected loss outputs for the commercial credit-risk portfolio.

## Portfolio role

`expected-loss-engine-commercial` is the downstream integration engine for expected loss.

## Upstream inputs

- `PD-and-scorecard-commercial`
- `LGD-commercial`
- `EAD-CCF-commercial`

Optional supporting reference inputs:
- `industry-analysis`

## Downstream consumers

- `stress-testing-commercial`
- `RAROC-pricing-and-return-hurdle`
- `portfolio-monitor-commercial` (planned downstream repo; not yet published on the public portfolio)
- `RWA-capital-commercial`

Some downstream modules are planned but not yet published on the public portfolio.

## Rebuilt deliverables

- Standard repo structure with `data`, `docs`, `notebooks`, `src`, `scripts`, `outputs`, and `tests`.
- End-to-end Codex demo pipeline: `python -m src.codex_run_pipeline`.
- Required output contract files in `outputs/tables`.
