import base64
from io import BytesIO
import matplotlib.pyplot as plt

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
