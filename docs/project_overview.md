# Project Overview - expected-loss-engine-commercial

This repo sits after the core commercial PD, LGD, and EAD components. It combines facility-level risk inputs into expected loss outputs, then rolls those results into portfolio reporting, pricing support, scenario stress testing, and capital-style downstream use.

The design goal is practical credit-risk workflow coverage rather than model-theory depth in isolation. A credit officer, portfolio manager, or private credit underwriter should be able to see how the same facility-level loss engine can support both day-to-day underwriting and book-level oversight.

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
