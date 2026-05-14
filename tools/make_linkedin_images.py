"""Generate the three LinkedIn images for the Gibran v0.0.2 post.

Run from the repo root:
    python tools/make_linkedin_images.py

Output: tools/img/{1_pipeline,2_no_invent,3_features}.png

These are intentionally simple matplotlib figures -- not marketing
polish. Plain typography, high contrast, fits LinkedIn's preview
crop (1.91:1 aspect ratio, ~1200x628).
"""
from __future__ import annotations

from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt


OUT_DIR = Path(__file__).parent / "img"
OUT_DIR.mkdir(exist_ok=True)


def _setup_figure(title: str) -> tuple[plt.Figure, plt.Axes]:
    fig, ax = plt.subplots(figsize=(12, 6.28), dpi=120)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 6.28)
    ax.axis("off")
    ax.text(
        0.5, 5.85, title, fontsize=22, fontweight="bold",
        family="DejaVu Sans", color="#0a2540",
    )
    return fig, ax


def _box(
    ax: plt.Axes, x: float, y: float, w: float, h: float, text: str,
    *, face: str = "#f6f9fc", edge: str = "#0a2540", fontsize: int = 11,
    fontweight: str = "normal",
) -> None:
    rect = mpatches.FancyBboxPatch(
        (x, y), w, h, boxstyle="round,pad=0.05",
        linewidth=1.5, edgecolor=edge, facecolor=face,
    )
    ax.add_patch(rect)
    ax.text(
        x + w / 2, y + h / 2, text, ha="center", va="center",
        fontsize=fontsize, fontweight=fontweight,
        family="DejaVu Sans", color="#0a2540",
    )


def _arrow(ax: plt.Axes, x1: float, y1: float, x2: float, y2: float, *,
           color: str = "#0a2540") -> None:
    ax.annotate(
        "", xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(arrowstyle="->", lw=2, color=color),
    )


# ---------------------------------------------------------------------------
# Image 1: NL -> DSL -> SQL pipeline
# ---------------------------------------------------------------------------

def make_pipeline_image() -> Path:
    fig, ax = _setup_figure("How `gibran ask` works")

    _box(ax, 0.3, 3.5, 2.6, 1.2,
         '"show me customer\nretention"',
         face="#fff4e6", edge="#8b4513", fontweight="bold")
    _arrow(ax, 2.95, 4.1, 3.4, 4.1)

    _box(ax, 3.45, 3.5, 2.6, 1.2,
         "Pattern match\n+ resolve slots\nagainst AllowedSchema",
         face="#e6f0ff", edge="#0a2540")
    _arrow(ax, 6.1, 4.1, 6.55, 4.1)

    _box(ax, 6.6, 3.5, 2.4, 1.2,
         "DSL intent\n(typed JSON)",
         face="#e6f0ff", edge="#0a2540")
    _arrow(ax, 9.05, 4.1, 9.5, 4.1)

    _box(ax, 9.55, 3.5, 2.2, 1.2,
         "Governed SQL\n(3-CTE cohort)",
         face="#d4edda", edge="#155724", fontweight="bold")

    # Fail-closed branch
    ax.text(
        0.5, 2.4, "If a slot doesn't resolve to a real reference on the schema:",
        fontsize=12, family="DejaVu Sans", color="#0a2540",
    )
    _box(ax, 0.5, 0.8, 5, 1.2,
         '"why did revenue drop\nlast week"',
         face="#fff4e6", edge="#8b4513", fontweight="bold")
    _arrow(ax, 5.55, 1.4, 6.0, 1.4)
    _box(ax, 6.1, 0.8, 5.4, 1.2,
         '→ "I don\'t know how to answer that."\nexit code 4 — never invents',
         face="#f8d7da", edge="#721c24", fontweight="bold")

    out = OUT_DIR / "1_pipeline.png"
    fig.savefig(out, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out


# ---------------------------------------------------------------------------
# Image 2: Gibran NL vs LLM NL (the no-invention contract)
# ---------------------------------------------------------------------------

def make_comparison_image() -> Path:
    fig, ax = _setup_figure("The no-hallucination contract")

    # Left column: Gibran
    _box(ax, 0.5, 4.0, 5.3, 0.9,
         "Gibran (pattern matcher)",
         face="#d4edda", edge="#155724",
         fontsize=14, fontweight="bold")

    gibran_rows = [
        ("Input: known metric", "✓ Compiles + runs"),
        ("Input: typo'd metric", '⚠ "I don\'t know"'),
        ("Input: invented metric", '⚠ "I don\'t know"'),
        ("Input: outside 6 patterns", '⚠ "I don\'t know"'),
    ]
    y = 3.5
    for left, right in gibran_rows:
        _box(ax, 0.5, y - 0.65, 3.3, 0.55, left,
             face="white", edge="#155724", fontsize=10)
        _box(ax, 3.85, y - 0.65, 1.95, 0.55, right,
             face="white", edge="#155724", fontsize=10)
        y -= 0.65

    # Right column: LLM-based NL
    _box(ax, 6.2, 4.0, 5.3, 0.9,
         "LLM-based NL (constrained or not)",
         face="#fff4e6", edge="#8b4513",
         fontsize=14, fontweight="bold")

    llm_rows = [
        ("Input: known metric", "✓ Often compiles"),
        ("Input: typo'd metric", "⚠ Fuzzy-matches plausibly"),
        ("Input: invented metric", "✗ Hallucinates a column"),
        ("Input: ambiguous question", "✗ Picks 'reasonable' answer"),
    ]
    y = 3.5
    for left, right in llm_rows:
        _box(ax, 6.2, y - 0.65, 3.0, 0.55, left,
             face="white", edge="#8b4513", fontsize=10)
        _box(ax, 9.25, y - 0.65, 2.25, 0.55, right,
             face="white", edge="#8b4513", fontsize=10)
        y -= 0.65

    # Bottom line
    ax.text(
        0.5, 0.4,
        '"⚠ I don\'t know" is a feature. It\'s the cost of the guarantee that '
        'no output reference is invented.',
        fontsize=11, fontstyle="italic", family="DejaVu Sans", color="#0a2540",
    )

    out = OUT_DIR / "2_no_invent.png"
    fig.savefig(out, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out


# ---------------------------------------------------------------------------
# Image 3: Feature summary
# ---------------------------------------------------------------------------

def make_features_image() -> Path:
    fig, ax = _setup_figure("Gibran v0.0.2 — at a glance")

    sections = [
        ("Metric vocabulary", [
            "19 primitives",
            "cohort_retention / funnel",
            "multi_stage_filter",
            "period_over_period",
            "percentile, rolling_window",
            "weighted_avg, mode, …",
        ], "#e6f0ff"),
        ("Governance", [
            "Row + column policies",
            "Identity-aware SQL rewrite",
            "Time-bound grants",
            "Break-glass roles",
            "Rate limiter",
            "Audit log + PII redaction",
        ], "#d4edda"),
        ("Operational", [
            "Schema-drift detection",
            "Anomaly rule type + webhooks",
            "Access-pattern anomalies",
            "Approval workflow",
            "Plan + result caching",
            "Materialized metrics",
        ], "#fff4e6"),
    ]

    section_width = 3.7
    gap = 0.2
    start_x = 0.4

    for i, (title, items, color) in enumerate(sections):
        x = start_x + i * (section_width + gap)
        _box(ax, x, 4.4, section_width, 0.7, title,
             face=color, edge="#0a2540",
             fontsize=14, fontweight="bold")
        for j, item in enumerate(items):
            y = 3.85 - j * 0.45
            ax.text(
                x + 0.15, y, f"✓ {item}",
                fontsize=11, family="DejaVu Sans", color="#0a2540",
            )

    # Footer
    ax.text(
        6.0, 0.8,
        "Embedded over DuckDB · No server · MIT · 456 passing tests",
        ha="center", fontsize=12, fontweight="bold",
        family="DejaVu Sans", color="#0a2540",
    )
    ax.text(
        6.0, 0.3,
        "pip install gibran   ·   github.com/AdamEzzat1/gibran",
        ha="center", fontsize=11, family="DejaVu Sans Mono",
        color="#0a2540",
    )

    out = OUT_DIR / "3_features.png"
    fig.savefig(out, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out


if __name__ == "__main__":
    for fn in (make_pipeline_image, make_comparison_image, make_features_image):
        path = fn()
        print(f"wrote {path}")
