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


def get_current_total() -> float:
    """查询最新一条记录获取当前 total size。"""
    resp = table.query(
        KeyConditionExpression="bucket_name = :bn",
        ExpressionAttributeValues={":bn": BUCKET_NAME},
        ScanIndexForward=False,  # 降序，最新的在最前
        Limit=1,
    )
    items = resp.get("Items", [])
    return Decimal(str(items[0]["size_bytes"])) if items else Decimal("0")


def handler(event, context):
    print("RAW EVENT:", json.dumps(event))

    for r in parse_s3_records(event):
        event_name = r["eventName"]
        obj_key    = r["s3"]["object"]["key"]
        obj_size   = r["s3"]["object"].get("size", 0)

        if "ObjectCreated" in event_name:
            delta = Decimal(str(obj_size))
        elif "ObjectRemoved" in event_name:
            delta = Decimal(str(-obj_size))
        else:
            print(f"未处理的事件类型: {event_name}，跳过")
            continue

        # 跳过 plot.png，不计入 size tracking
        if obj_key == "plot.png":
            print(f"跳过 plot.png，不计入 size tracking")
            continue

        # 获取当前总大小，加上 delta
        current_total = get_current_total()
        new_total     = current_total + delta

        table.put_item(Item={
            "bucket_name": BUCKET_NAME,
            "timestamp":   timestamp,
            "size_bytes":  new_total,
            "event_name":  event_name,
            "object_key":  obj_key,
        })

        print(f"[{event_name}] {obj_key} delta={delta} → total={new_total}")