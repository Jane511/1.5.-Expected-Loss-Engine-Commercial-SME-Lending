from __future__ import annotations

import numpy as np
import pandas as pd

from .config import PRICING_ASSUMPTIONS
from .utils import weighted_average


def apply_pricing(
    df: pd.DataFrame,
    pricing_assumptions: dict[str, float] | None = None,
) -> pd.DataFrame:
    assumptions = pricing_assumptions or PRICING_ASSUMPTIONS
    out = df.copy()
    if "interest_rate" not in out.columns:
        out["interest_rate"] = np.nan
    out["interest_rate"] = pd.to_numeric(out["interest_rate"], errors="coerce")
    out["funding_cost"] = assumptions["funding_cost"]
    out["operating_cost"] = assumptions["operating_cost"]
    out["target_return"] = assumptions["target_return"]
    out["required_margin"] = (
        out["el_rate"] + out["funding_cost"] + out["operating_cost"] + out["target_return"]
    ).round(6)
    out["pricing_gap"] = (out["interest_rate"] - out["required_margin"]).round(6)
    out["pricing_status"] = np.select(
        [
            out["pricing_gap"] >= 0.005,
            out["pricing_gap"] >= 0.0,
        ],
        [
            "Above hurdle",
            "Meets hurdle",
        ],
        default="Below hurdle",
    )
    return out


def summarise_pricing(df: pd.DataFrame) -> pd.DataFrame:
    records: list[dict] = []
    for product_type, group in df.groupby("product_type", sort=True):
        records.append(
            {
                "product_type": product_type,
                "facility_count": int(len(group)),
                "total_ead": round(float(group["ead"].sum()), 2),
                "el_rate": round(weighted_average(group["el_rate"], group["ead"]), 6),
                "average_interest_rate": round(weighted_average(group["interest_rate"], group["ead"]), 6),
                "required_margin": round(weighted_average(group["required_margin"], group["ead"]), 6),
                "pricing_gap": round(weighted_average(group["pricing_gap"], group["ead"]), 6),
                "share_meeting_hurdle": round(float((group["pricing_gap"] >= 0).mean()), 4),
            }
        )
    return pd.DataFrame.from_records(records)
