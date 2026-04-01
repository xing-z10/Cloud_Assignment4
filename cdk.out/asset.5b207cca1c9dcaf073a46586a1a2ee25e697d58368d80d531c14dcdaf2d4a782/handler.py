import json
import os
import boto3
from boto3.dynamodb.conditions import Attr

BUCKET_NAME = os.environ["BUCKET_NAME"]
TABLE_NAME  = os.environ["TABLE_NAME"]

dynamodb = boto3.resource("dynamodb")
table    = dynamodb.Table(TABLE_NAME)


def parse_s3_records(event):
    """从 SQS → SNS → S3 的三层嵌套结构中解析出 S3 records。"""
    records = []
    for sqs_record in event["Records"]:
        sns_body  = json.loads(sqs_record["body"])
        s3_event  = json.loads(sns_body["Message"])
        for r in s3_event.get("Records", []):
            records.append(r)
    return records


def handler(event, context):
    for r in parse_s3_records(event):
        event_name = r["eventName"]
        obj_key    = r["s3"]["object"]["key"]
        obj_size   = r["s3"]["object"].get("size", 0)

        if "ObjectCreated" in event_name:
            delta = obj_size
        elif "ObjectRemoved" in event_name:
            delta = -obj_size   # S3 删除事件 size 字段为 0，delta 也为 0
        else:
            print(f"Unhandled event type: {event_name}, skipping.")
            continue

        # 用 ADD 原子操作更新 total_size，item 不存在时自动创建
        table.update_item(
            Key={"bucket_name": BUCKET_NAME},
            UpdateExpression="ADD total_size :delta",
            ExpressionAttributeValues={":delta": delta},
        )

        new_size = table.get_item(
            Key={"bucket_name": BUCKET_NAME}
        )["Item"]["total_size"]

        print(f"[{event_name}] {obj_key} delta={delta} → total={new_size}")