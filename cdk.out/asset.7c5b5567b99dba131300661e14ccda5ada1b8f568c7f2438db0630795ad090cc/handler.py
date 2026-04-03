import json
import os
import boto3
from decimal import Decimal
from datetime import datetime, timezone

BUCKET_NAME = os.environ["BUCKET_NAME"]
TABLE_NAME  = os.environ["TABLE_NAME"]

s3       = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table    = dynamodb.Table(TABLE_NAME)


def get_current_total() -> float:
    """查询最新一条记录获取当前 total size。"""
    resp = table.query(
        KeyConditionExpression="bucket_name = :bn",
        ExpressionAttributeValues={":bn": BUCKET_NAME},
        ScanIndexForward=False,
        Limit=1,
    )
    items = resp.get("Items", [])
    return Decimal(str(items[0]["size_bytes"])) if items else Decimal("0")


def handler(event, context):
    # Cleaner 由 CloudWatch Alarm → SNS 触发，不需要解析 event 内容
    print(f"Cleaner triggered. Event: {event}")

    response = s3.list_objects_v2(Bucket=BUCKET_NAME)

    if "Contents" not in response or len(response["Contents"]) == 0:
        print("Bucket 为空，无需删除。")
        return

    # 过滤掉 plot.png，不参与删除逻辑
    contents = [obj for obj in response["Contents"] if obj["Key"] != "plot.png"]

    if not contents:
        print("没有可删除的对象（排除 plot.png 后为空）。")
        return

    # 找到最大的对象并删除
    largest = max(contents, key=lambda o: o["Size"])
    key     = largest["Key"]
    size    = largest["Size"]

    print(f"删除最大对象: {key} ({size} bytes)")
    s3.delete_object(Bucket=BUCKET_NAME, Key=key)

    # 写入新记录，total_size 减去被删除对象的大小
    current_total = get_current_total()
    new_total = current_total - Decimal(str(size))
    timestamp     = datetime.now(timezone.utc).isoformat()

    table.put_item(Item={
        "bucket_name": BUCKET_NAME,
        "timestamp":   timestamp,
        "size_bytes":  new_total,
        "event_name":  "ObjectRemoved:Delete",
        "object_key":  key,
    })

    print(f"已删除 {key}，新 total = {new_total} bytes。")