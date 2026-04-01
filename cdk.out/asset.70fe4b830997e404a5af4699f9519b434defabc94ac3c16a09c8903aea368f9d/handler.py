import os
import time
import urllib.request
import boto3

BUCKET_NAME    = os.environ["BUCKET_NAME"]
PLOTTER_API    = os.environ.get("PLOTTER_API_URL", "")   # CDK output 注入

s3 = boto3.client("s3")


def put_object(key: str, body: str):
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=body.encode("utf-8"))
    print(f"Created {key} ({len(body.encode('utf-8'))} bytes)")


def handler(event, context):
    # ── Step 1: upload assignment1.txt (18 bytes) ──────────────────────
    put_object("assignment1.txt", "Empty Assignment 1")
    # total = 18, alarm not yet fired

    time.sleep(15)   # 等 SNS/SQS/Lambda 处理完

    # ── Step 2: upload assignment2.txt (28 bytes) ──────────────────────
    put_object("assignment2.txt", "Empty Assignment 2222222222")
    # total = 18 + 28 = 46 > 20 → alarm 应触发 → Cleaner 删除 assignment2.txt

    time.sleep(90)   # 等 alarm 评估周期 + Cleaner 执行完
    # 预期: assignment2.txt 被删除, total ≈ 18

    # ── Step 3: upload assignment3.txt (2 bytes) ───────────────────────
    put_object("assignment3.txt", "33")
    # total = 18 + 2 = 20, 不超过阈值（> 20 才触发），alarm 不应再触发
    # 但如果 alarm 周期内累积超过 20，Cleaner 会删除 assignment1.txt

    time.sleep(90)   # 等待 alarm 评估完成

    # ── Step 4: 调用 Plotter API ────────────────────────────────────────
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