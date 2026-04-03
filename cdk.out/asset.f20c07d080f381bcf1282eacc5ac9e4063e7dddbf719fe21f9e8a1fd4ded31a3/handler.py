import os
import time
import urllib.request
import boto3

BUCKET_NAME = os.environ["BUCKET_NAME"]
PLOTTER_API = os.environ.get("PLOTTER_API_URL", "")

s3 = boto3.client("s3")


def put_object(key: str, body: str):
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=body.encode("utf-8"))
    print(f"Created {key} ({len(body.encode('utf-8'))} bytes)")


def handler(event, context):
    # ── Step 1: upload assignment1.txt (18 bytes) ──────────────────────
    # total = 18, alarm 不触发
    put_object("assignment1.txt", "Empty Assignment 1")

    time.sleep(10)  # 等 SNS/SQS/Lambda 处理完

    # ── Step 2: upload assignment2.txt (28 bytes) ──────────────────────
    # total = 18 + 28 = 46 > 20 → alarm 触发 → Cleaner 删除 assignment2.txt
    put_object("assignment2.txt", "Empty Assignment 2222222222")

    time.sleep(30)  # 等待 alarm 评估周期(10秒) + Cleaner 执行
    # 预期: assignment2.txt 被删除, total = 18

    # ── Step 3: upload assignment3.txt (2 bytes) ───────────────────────
    # total = 18 + 2 = 20, 不超过阈值不触发
    # 但 logging lambda 记录的 size_delta 累加后 > 20 → alarm 再次触发
    # → Cleaner 删除 assignment1.txt
    put_object("assignment3.txt", "33")

    time.sleep(30)  # 等待第二次 alarm 触发 + Cleaner 执行

    # ── Step 4: 调用 Plotter API ───────────────────────────────────────
    if PLOTTER_API:
        print(f"Calling plotter API: {PLOTTER_API}")
        try:
            with urllib.request.urlopen(PLOTTER_API, timeout=30) as resp:
                body = resp.read().decode("utf-8")
                print(f"Plotter response: {body[:500]}")
        except Exception as e:
            print(f"Plotter API call failed: {e}")
    else:
        print("PLOTTER_API_URL not set, skipping plotter call.")

    print("Driver finished.")