from __future__ import annotations

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import pandas as pd
import numpy as np


CHART_DIR_DEFAULT = Path(__file__).resolve().parents[1] / "outputs" / "charts"

PALETTE = {
    "primary": "#1a5276",
    "secondary": "#2e86c1",
    "accent": "#27ae60",
    "warning": "#e67e22",
    "danger": "#c0392b",
    "neutral": "#7f8c8d",
    "stage1": "#27ae60",
    "stage2": "#e67e22",
    "stage3": "#c0392b",
}


def _setup_chart_dir(chart_dir: Path | None = None) -> Path:
    d = chart_dir or CHART_DIR_DEFAULT
    d.mkdir(parents=True, exist_ok=True)
    return d


def plot_el_by_risk_grade(df: pd.DataFrame, chart_dir: Path | None = None) -> Path:
    """Bar chart of expected loss by risk grade."""
    d = _setup_chart_dir(chart_dir)
    rg_col = "risk_grade" if "risk_grade" in df.columns else "internal_risk_grade"
    el_col = "expected_loss" if "expected_loss" in df.columns else "ecl_provision"

    agg = df.groupby(rg_col, sort=True).agg(
        total_el=(el_col, "sum"),
        total_ead=("ead", "sum"),
        count=("facility_id", "count"),
    ).reset_index()

    fig, ax1 = plt.subplots(figsize=(9, 5))
    x = range(len(agg))
    bars = ax1.bar(x, agg["total_el"], color=PALETTE["primary"], alpha=0.85, label="Expected Loss ($)")
    ax1.set_xlabel("Risk Grade")
    ax1.set_ylabel("Expected Loss ($)", color=PALETTE["primary"])
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(agg[rg_col])
    ax1.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"${v:,.0f}"))

    ax2 = ax1.twinx()
    el_rate = (agg["total_el"] / agg["total_ead"].replace(0, np.nan)).fillna(0)
    ax2.plot(x, el_rate * 100, color=PALETTE["danger"], marker="o", linewidth=2, label="EL Rate (%)")
    ax2.set_ylabel("EL Rate (%)", color=PALETTE["danger"])
    ax2.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=1))

    fig.suptitle("Expected Loss by Risk Grade", fontsize=13, fontweight="bold")
    fig.legend(loc="lower center", ncol=2, bbox_to_anchor=(0.5, -0.02))
    fig.tight_layout()
    path = d / "el_by_risk_grade.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_el_waterfall(df: pd.DataFrame, chart_dir: Path | None = None) -> Path:
    """Waterfall chart showing EL contribution by product and total portfolio EL."""
    d = _setup_chart_dir(chart_dir)
    el_col = "expected_loss" if "expected_loss" in df.columns else "ecl_provision"
    group_col = "product_type" if "product_type" in df.columns else "risk_grade"

    agg = (
        df.groupby(group_col, dropna=False, sort=False)[el_col]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    agg[group_col] = agg[group_col].fillna("Unknown").astype(str)
    agg["start"] = agg[el_col].cumsum() - agg[el_col]
    agg["end"] = agg["start"] + agg[el_col]
    total_el = float(agg[el_col].sum())

    fig, ax = plt.subplots(figsize=(max(9, len(agg) * 1.6), 5.5))
    x = np.arange(len(agg))

    ax.bar(
        x,
        agg[el_col],
        bottom=agg["start"],
        color=PALETTE["secondary"],
        alpha=0.85,
        edgecolor="white",
        label="Contribution",
    )

    for i in range(1, len(agg)):
        connector_level = agg.iloc[i - 1]["end"]
        ax.plot(
            [i - 1 + 0.4, i - 0.4],
            [connector_level, connector_level],
            color=PALETTE["neutral"],
            linewidth=1.2,
            linestyle="--",
        )

    total_x = len(agg)
    ax.bar(
        total_x,
        total_el,
        color=PALETTE["primary"],
        alpha=0.95,
        edgecolor="white",
        label="Total EL",
    )

    y_offset = max(total_el * 0.015, 500)
    for i, row in agg.iterrows():
        ax.text(
            i,
            row["end"] + y_offset,
            f"${row[el_col]:,.0f}",
            ha="center",
            va="bottom",
            fontsize=9,
            rotation=0,
        )
    ax.text(
        total_x,
        total_el + y_offset,
        f"${total_el:,.0f}",
        ha="center",
        va="bottom",
        fontsize=9,
        fontweight="bold",
    )

    labels = agg[group_col].tolist() + ["Total EL"]
    ax.set_xticks(np.arange(len(labels)))
    ax.set_xticklabels(labels, rotation=20, ha="right")
    ax.set_ylabel("Expected Loss ($)")
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    ax.set_title("Expected Loss Waterfall", fontsize=13, fontweight="bold")
    ax.legend()
    fig.tight_layout()
    path = d / "el_waterfall.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_el_by_product(df: pd.DataFrame, chart_dir: Path | None = None) -> Path:
    """Horizontal bar chart of EAD and EL by product type."""
    d = _setup_chart_dir(chart_dir)
    el_col = "expected_loss" if "expected_loss" in df.columns else "ecl_provision"

    agg = df.groupby("product_type", sort=False).agg(
        total_ead=("ead", "sum"),
        total_el=(el_col, "sum"),
    ).reset_index().sort_values("total_ead", ascending=True)

    fig, ax = plt.subplots(figsize=(9, 5))
    y = range(len(agg))
    ax.barh(y, agg["total_ead"], color=PALETTE["secondary"], alpha=0.7, label="EAD")
    ax.barh(y, agg["total_el"], color=PALETTE["danger"], alpha=0.9, label="Expected Loss")
    ax.set_yticks(list(y))
    ax.set_yticklabels(agg["product_type"])
    ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    ax.set_xlabel("Amount ($)")
    ax.set_title("EAD and Expected Loss by Product", fontsize=13, fontweight="bold")
    ax.legend()
    fig.tight_layout()
    path = d / "el_by_product.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_ifrs9_stage_breakdown(stage_summary: pd.DataFrame, chart_dir: Path | None = None) -> Path:
    """Pie/donut chart showing ECL provision split by IFRS 9 stage."""
    d = _setup_chart_dir(chart_dir)
    data = stage_summary[stage_summary["ifrs9_stage"] > 0].copy()

    colors = [PALETTE[f"stage{s}"] for s in data["ifrs9_stage"]]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    wedges1, _, autotexts1 = ax1.pie(
        data["total_ead"], labels=data["stage_label"],
        autopct="%1.1f%%", colors=colors, startangle=90,
        wedgeprops={"width": 0.55, "edgecolor": "white"},
    )
    ax1.set_title("EAD by Stage", fontsize=12, fontweight="bold")

    wedges2, _, autotexts2 = ax2.pie(
        data["total_ecl_provision"], labels=data["stage_label"],
        autopct="%1.1f%%", colors=colors, startangle=90,
        wedgeprops={"width": 0.55, "edgecolor": "white"},
    )
    ax2.set_title("ECL Provision by Stage", fontsize=12, fontweight="bold")

    fig.suptitle("IFRS 9 Stage Distribution", fontsize=14, fontweight="bold", y=1.02)
    fig.tight_layout()
    path = d / "ifrs9_stage_breakdown.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_concentration_heatmap(sector_df: pd.DataFrame, chart_dir: Path | None = None) -> Path:
    """Heatmap of sector concentration showing EAD share and EL share."""
    d = _setup_chart_dir(chart_dir)

    data = sector_df.sort_values("total_ead", ascending=False).head(9)
    matrix = data[["ead_share", "el_share"]].values

    fig, ax = plt.subplots(figsize=(7, max(4, len(data) * 0.55)))
    im = ax.imshow(matrix, cmap="YlOrRd", aspect="auto", vmin=0, vmax=max(0.35, matrix.max()))

    ax.set_xticks([0, 1])
    ax.set_xticklabels(["EAD Share", "EL Share"])
    ax.set_yticks(range(len(data)))
    ax.set_yticklabels(data["industry"].values)

    for i in range(len(data)):
        for j in range(2):
            val = matrix[i, j]
            color = "white" if val > 0.18 else "black"
            ax.text(j, i, f"{val:.1%}", ha="center", va="center", color=color, fontsize=10)

    ax.set_title("Sector Concentration Heatmap", fontsize=13, fontweight="bold")
    fig.colorbar(im, ax=ax, label="Share", shrink=0.8)
    fig.tight_layout()
    path = d / "concentration_heatmap.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_stress_test_comparison(stress_df: pd.DataFrame, chart_dir: Path | None = None) -> Path:
    """Grouped bar chart comparing EL across stress scenarios."""
    d = _setup_chart_dir(chart_dir)

    fig, ax = plt.subplots(figsize=(9, 5))
    colors = [PALETTE["accent"], PALETTE["warning"], PALETTE["danger"]]

    scenarios = stress_df["scenario"].values
    x = range(len(scenarios))
    bars = ax.bar(x, stress_df["total_el"], color=colors[:len(scenarios)], alpha=0.85, edgecolor="white")

    for bar, val in zip(bars, stress_df["total_el"]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                f"${val:,.0f}", ha="center", va="bottom", fontsize=10, fontweight="bold")

    ax.set_xticks(list(x))
    ax.set_xticklabels(scenarios)
    ax.set_ylabel("Total Expected Loss ($)")
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    ax.set_title("Stress Test: Expected Loss by Scenario", fontsize=13, fontweight="bold")
    fig.tight_layout()
    path = d / "stress_test_comparison.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def generate_all_charts(pipeline_results: dict, chart_dir: Path | None = None) -> list[Path]:
    """Generate the full chart pack from pipeline results.

    Accepts the dict returned by pipeline.run_pipeline() or a dict containing
    the relevant DataFrames with keys: loan_level_el, stress_results,
    ifrs9_stage_summary, sector_concentration.
    """
    paths = []
    d = _setup_chart_dir(chart_dir)

    el_df = pipeline_results.get("loan_level_el")
    if el_df is None:
        el_df = pipeline_results.get("ifrs9_el")
    if el_df is not None and not el_df.empty:
        paths.append(plot_el_waterfall(el_df, d))
        paths.append(plot_el_by_risk_grade(el_df, d))
        paths.append(plot_el_by_product(el_df, d))

    stage_df = pipeline_results.get("ifrs9_stage_summary")
    if stage_df is not None and not stage_df.empty:
        paths.append(plot_ifrs9_stage_breakdown(stage_df, d))

    sector_df = pipeline_results.get("sector_concentration")
    if sector_df is not None and not sector_df.empty:
        paths.append(plot_concentration_heatmap(sector_df, d))

    stress_df = pipeline_results.get("stress_results")
    if stress_df is not None and not stress_df.empty:
        paths.append(plot_stress_test_comparison(stress_df, d))

    return paths
