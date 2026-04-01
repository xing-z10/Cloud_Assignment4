import os
import json
import base64
import boto3
import matplotlib
matplotlib.use("Agg")   # 无头模式，Lambda 环境没有 display
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timezone, timedelta
from io import BytesIO

BUCKET_NAME = os.environ["BUCKET_NAME"]

cw = boto3.client("cloudwatch")


def get_metric_datapoints(hours: int = 1):
    """
    从 CloudWatch 拉取 TotalObjectSize 的原始数据点。
    用 SampleCount statistic 拿到每个时间点的 size_delta，
    再累加还原出 total size 随时间的变化。
    """
    end_time   = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours)

    resp = cw.get_metric_statistics(
        Namespace="Assignment4App",
        MetricName="TotalObjectSize",
        StartTime=start_time,
        EndTime=end_time,
        Period=60,          # 每 1 分钟一个数据点
        Statistics=["Sum"],
    )

    datapoints = sorted(resp["Datapoints"], key=lambda d: d["Timestamp"])
    return datapoints


def build_cumulative(datapoints):
    """把每个周期的 Sum(size_delta) 累加，得到 total size 随时间的曲线。"""
    times  = []
    totals = []
    total  = 0
    for dp in datapoints:
        total += dp["Sum"]
        times.append(dp["Timestamp"])
        totals.append(total)
    return times, totals


def render_plot(times, totals) -> str:
    """生成折线图，返回 base64 编码的 PNG。"""
    fig, ax = plt.subplots(figsize=(8, 4))

    if times:
        ax.step(times, totals, where="post", color="#4f86c6", linewidth=2)
        ax.axhline(y=20, color="#e05c5c", linestyle="--", linewidth=1, label="Alarm threshold (20)")
        ax.fill_between(times, totals, step="post", alpha=0.15, color="#4f86c6")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        plt.xticks(rotation=30, ha="right")
    else:
        ax.text(0.5, 0.5, "No data points found", transform=ax.transAxes,
                ha="center", va="center", fontsize=12, color="gray")

    ax.set_title(f"Total object size in {BUCKET_NAME}")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Total size (bytes)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()

    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=120)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def handler(event, context):
    datapoints      = get_metric_datapoints(hours=1)
    times, totals   = build_cumulative(datapoints)
    img_b64         = render_plot(times, totals)

    # 返回内嵌图片的 HTML 页面
    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Assignment 4 Plot</title></head>
<body style="font-family:sans-serif;padding:24px;background:#f9f9f9">
  <h2>Total object size over time</h2>
  <img src="data:image/png;base64,{img_b64}" style="max-width:100%;border:1px solid #ddd;border-radius:8px"/>
  <p style="color:#888;font-size:12px">Generated at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
</body>
</html>"""

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "text/html"},
        "body": html,
    }