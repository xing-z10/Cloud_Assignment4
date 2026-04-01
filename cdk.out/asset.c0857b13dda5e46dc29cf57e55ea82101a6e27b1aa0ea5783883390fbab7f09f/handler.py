import json
import os
import logging
import boto3

BUCKET_NAME    = os.environ["BUCKET_NAME"]
LOG_GROUP_NAME = f"/aws/lambda/{os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'LoggingFunction')}"

logs_client = boto3.client("logs")
logger      = logging.getLogger()
logger.setLevel(logging.INFO)


def parse_s3_records(event):
    records = []
    for sqs_record in event["Records"]:
        sns_body = json.loads(sqs_record["body"])
        s3_event = json.loads(sns_body["Message"])
        for r in s3_event.get("Records", []):
            records.append(r)
    return records


def get_creation_size(obj_key: str) -> int:
    """
    查询本 Lambda 的日志组，找到该对象的创建日志，返回其 size_delta。
    用于删除事件——S3 ObjectRemoved 不携带对象大小。
    """
    response = logs_client.filter_log_events(
        logGroupName=LOG_GROUP_NAME,
        filterPattern=f'{{$.object_name = "{obj_key}" && $.size_delta > 0}}',
    )
    for event in response.get("events", []):
        # Lambda 日志格式: "2024-01-01T00:00:00Z\tREQUEST_ID\t{...json...}\n"
        # 取最后一个 tab 之后的部分
        raw = event["message"].strip()
        parts = raw.split("\t")
        json_str = parts[-1] if len(parts) >= 3 else raw
        try:
            record = json.loads(json_str)
            if "size_delta" in record and record["size_delta"] > 0:
                return int(record["size_delta"])
        except (json.JSONDecodeError, KeyError):
            continue
    return 0


def handler(event, context):
    for r in parse_s3_records(event):
        event_name = r["eventName"]
        obj_key    = r["s3"]["object"]["key"]

        if "ObjectCreated" in event_name:
            size_delta = r["s3"]["object"].get("size", 0)
        elif "ObjectRemoved" in event_name:
            creation_size = get_creation_size(obj_key)
            size_delta    = -creation_size
        else:
            print(f"Unhandled event type: {event_name}, skipping.")
            continue

        # 用 logger.info 写入 CloudWatch，供 MetricFilter 提取
        logger.info(json.dumps({
            "object_name": obj_key,
            "size_delta":  size_delta,
        }))