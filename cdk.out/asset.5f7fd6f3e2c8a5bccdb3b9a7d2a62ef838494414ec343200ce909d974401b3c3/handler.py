import json
import os
import boto3
from decimal import Decimal
from datetime import datetime, timezone

BUCKET_NAME = os.environ["BUCKET_NAME"]
TABLE_NAME  = os.environ["TABLE_NAME"]

dynamodb = boto3.resource("dynamodb")
table    = dynamodb.Table(TABLE_NAME)


def parse_s3_records(event):
    """从 SQS → SNS → S3 的三层嵌套结构中解析出 S3 records。"""
    records = []
    for sqs_record in event["Records"]:
        sns_body = json.loads(sqs_record["body"])
        s3_event = json.loads(sns_body["Message"])
        for r in s3_event.get("Records", []):
            records.append(r)
    return records


def get_current_total() -> Decimal:
    """查询最新一条记录获取当前 total size。"""
    resp = table.query(
        KeyConditionExpression="bucket_name = :bn",
        ExpressionAttributeValues={":bn": BUCKET_NAME},
        ScanIndexForward=False,
        Limit=1,
    )
    items = resp.get("Items", [])
    return Decimal(str(items[0]["size_bytes"])) if items else Decimal("0")


def get_object_creation_size(obj_key: str) -> Decimal:
    """
    查找该对象最近一次创建时存储的 object_size。
    用于 ObjectRemoved 事件（S3 删除事件不含对象大小）。
    """
    resp = table.scan(
        FilterExpression="object_key = :key AND event_name = :en",
        ExpressionAttributeValues={
            ":key": obj_key,
            ":en":  "ObjectCreated:Put",
        },
    )
    items = resp.get("Items", [])
    if not items:
        print(f"未找到 {obj_key} 的创建记录，大小默认为 0")
        return Decimal("0")
    latest = max(items, key=lambda x: x["timestamp"])
    return Decimal(str(latest.get("object_size", 0)))


def handler(event, context):
    print("RAW EVENT:", json.dumps(event))

    for r in parse_s3_records(event):
        event_name = r["eventName"]
        obj_key    = r["s3"]["object"]["key"]
        obj_size   = r["s3"]["object"].get("size", 0)

        # 跳过 plot.png
        if obj_key == "plot.png":
            print("跳过 plot.png，不计入 size tracking")
            continue

        if "ObjectCreated" in event_name:
            delta = Decimal(str(obj_size))
        elif "ObjectRemoved" in event_name:
            # 删除事件 size=0，从历史记录查原始大小
            creation_size = get_object_creation_size(obj_key)
            delta = -creation_size
            print(f"删除事件 {obj_key}，找到原始大小: {creation_size}")
        else:
            print(f"未处理的事件类型: {event_name}，跳过")
            continue

        # 获取当前总大小，加上 delta
        current_total = get_current_total()
        new_total     = current_total + delta

        # 写入新记录，额外存储 object_size 供后续删除事件查询
        timestamp = datetime.now(timezone.utc).isoformat()
        table.put_item(Item={
            "bucket_name": BUCKET_NAME,
            "timestamp":   timestamp,
            "size_bytes":  new_total,
            "event_name":  event_name,
            "object_key":  obj_key,
            "object_size": Decimal(str(obj_size)),
        })

        print(f"[{event_name}] {obj_key} delta={delta} → total={new_total}")