import base64
from io import BytesIO

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def defected_pieces_per_machine_bar_chart_base64(rows) -> str | None:
    """
    Generate a bar chart for defected (NOK) pieces per machine.
    rows: list of dicts with keys 'machine_id' and 'nok'.
    Returns base64 PNG string.
    """
    if not rows:
        return None

    machines = [str(r['machine_id']) for r in rows]
    nok_counts = [r['nok'] for r in rows]

    fig = plt.figure(figsize=(6, 3), dpi=140)
    ax = fig.add_subplot(111)
    bars = ax.bar(machines, nok_counts, color="#c62828")
    ax.set_title("Defected Products per Machine")
    ax.set_ylabel("Defected (NOK)")
    ax.set_xlabel("Machine")
    ax.set_ylim(bottom=0)
    for bar, v in zip(bars, nok_counts):
        ax.text(bar.get_x() + bar.get_width() / 2, v + 0.5, str(v), ha='center', va='bottom', fontweight='bold', fontsize=9)
    fig.tight_layout()

    buf = BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    return b64

def hourly_bar_chart_base64(hourly_series: list[dict]) -> str | None:
    if not hourly_series:
        return None

    labels = [p["label"] for p in hourly_series]
    values = [p["pieces"] for p in hourly_series]

    fig = plt.figure(figsize=(9, 2.6), dpi=140)
    ax = fig.add_subplot(111)
    ax.bar(labels, values)

    ax.set_title("Hourly Output (last hours)")
    ax.set_ylabel("Pieces")
    ax.set_xlabel("Hour")
    ax.tick_params(axis="x", labelrotation=0)

    fig.tight_layout()

    buf = BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)

    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    return b64
