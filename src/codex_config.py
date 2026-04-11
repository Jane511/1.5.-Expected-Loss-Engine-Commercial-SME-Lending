from pathlib import Path
PROJECT_ROOT=Path(__file__).resolve().parents[1]
REPO_NAME='expected-loss-engine-commercial'
PIPELINE_KIND='el'
EXPECTED_OUTPUTS=['expected_loss_by_facility.csv', 'expected_loss_by_borrower.csv', 'expected_loss_by_segment.csv', 'portfolio_expected_loss.csv', 'scenario_weighted_ecl.csv']
