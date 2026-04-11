from __future__ import annotations

import pandas as pd

from .utils import weighted_average


def _summarise_group(df: pd.DataFrame, group_fields: list[str]) -> pd.DataFrame:
    working = df.copy()
    for column in ["limit_amount", "drawn_balance", "interest_rate"]:
        if column not in working.columns:
            working[column] = pd.NA

    records: list[dict] = []
    for keys, group in working.groupby(group_fields, dropna=False, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_fields, keys, strict=False))
        row.update(
            {
                "facility_count": int(len(group)),
                "total_limit_amount": round(float(group["limit_amount"].sum()), 2),
                "total_drawn_balance": round(float(group["drawn_balance"].sum()), 2),
                "total_ead": round(float(group["ead"].sum()), 2),
                "total_el": round(float(group["expected_loss"].sum()), 2),
                "avg_pd": round(weighted_average(group["pd_final"], group["ead"]), 6),
                "avg_lgd": round(weighted_average(group["lgd_final"], group["ead"]), 6),
                "weighted_interest_rate": round(weighted_average(group["interest_rate"], group["ead"]), 6),
            }
        )
        row["el_rate"] = round(row["total_el"] / row["total_ead"], 6) if row["total_ead"] else 0.0
        records.append(row)
    return pd.DataFrame.from_records(records)


def summarise_segment_expected_loss(df: pd.DataFrame) -> pd.DataFrame:
    return _summarise_group(df, ["product_type", "industry", "region", "risk_grade"])


def summarise_portfolio(df: pd.DataFrame) -> pd.DataFrame:
    product_summary = _summarise_group(df, ["product_type"])
    total_row = pd.DataFrame(
        [
            {
                "product_type": "Total Portfolio",
                "facility_count": int(len(df)),
                "total_limit_amount": round(float(df["limit_amount"].sum()), 2),
                "total_drawn_balance": round(float(df["drawn_balance"].sum()), 2),
                "total_ead": round(float(df["ead"].sum()), 2),
                "total_el": round(float(df["expected_loss"].sum()), 2),
                "avg_pd": round(weighted_average(df["pd_final"], df["ead"]), 6),
                "avg_lgd": round(weighted_average(df["lgd_final"], df["ead"]), 6),
                "weighted_interest_rate": round(weighted_average(df["interest_rate"], df["ead"]), 6),
                "el_rate": round(float(df["expected_loss"].sum()) / float(df["ead"].sum()), 6),
            }
        ]
    )
    return pd.concat([product_summary, total_row], ignore_index=True)
