# Validation Framework - expected-loss-engine-commercial

The canonical validation pack now checks:

- required output tables exist and are non-empty
- PD and LGD are within `[0, 1]`
- expected loss and EAD are non-negative
- IFRS 9 stages stay within the supported set
- the stress output contains the base, mild, and severe scenarios
- one input strategy was explicitly selected

The pipeline also writes `input_source_report.csv` so a reviewer can see whether the run used:

- current upstream repo exports
- a local staged input bundle
- generated demo data

Manual review should still confirm model assumptions, upstream/downstream contracts, and any rejected upstream input candidates recorded in the input-source report before portfolio or employer review.
