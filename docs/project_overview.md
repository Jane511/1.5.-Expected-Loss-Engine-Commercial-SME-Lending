# Project Overview

`expected-loss-engine-commercial` is the expected-loss integration engine in the public commercial credit-risk stack.

## Portfolio role

It combines PD, LGD, and EAD outputs into facility- and portfolio-level expected loss views that can be reused across stress testing, monitoring, pricing, and capital workflows.

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

## Products Covered

- SME Cash Flow Term Loan
- Property Backed Loan
- Overdraft / Revolving Working Capital

## Why This Repo Matters

- It translates model outputs into decision-ready loss metrics.
- It shows how PD, LGD, and EAD outputs can be combined into a coherent expected-loss layer.
- It links loss estimation to pricing and stress testing, which is how the metric is actually used in lending institutions.
