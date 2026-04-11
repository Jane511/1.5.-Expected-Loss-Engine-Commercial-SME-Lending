# Data Dictionary - expected-loss-engine-commercial

| Field | Description |
| --- | --- |
| `borrower_id` | Synthetic borrower identifier. |
| `facility_id` | Synthetic facility identifier. |
| `segment` | Portfolio segment. |
| `industry` | Australian industry grouping. |
| `product_type` | Facility or product type. |
| `limit` | Approved or committed exposure limit. |
| `drawn` | Current drawn balance. |
| `pd` | Demonstration PD input. |
| `lgd` | Demonstration LGD input. |
| `ead` | Demonstration EAD input. |

## Output files

- `outputs/tables/expected_loss_by_facility.csv`
- `outputs/tables/expected_loss_by_borrower.csv`
- `outputs/tables/expected_loss_by_segment.csv`
- `outputs/tables/portfolio_expected_loss.csv`
- `outputs/tables/scenario_weighted_ecl.csv`
