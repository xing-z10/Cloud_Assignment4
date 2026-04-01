import json
import os
import boto3

BUCKET_NAME = os.environ["BUCKET_NAME"]
TABLE_NAME  = os.environ["TABLE_NAME"]

s3       = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table    = dynamodb.Table(TABLE_NAME)


def handler(event, context):
    # Cleaner 由 CloudWatch Alarm → SNS 触发
    # 不需要解析 event 内容，直接找最大对象删除
    print(f"Cleaner triggered by alarm. Event: {event}")

    response = s3.list_objects_v2(Bucket=BUCKET_NAME)

    if "Contents" not in response or len(response["Contents"]) == 0:
        print("Bucket is empty, nothing to delete.")
        return

    # 找到最大的对象
    largest = max(response["Contents"], key=lambda o: o["Size"])
    key     = largest["Key"]
    size    = largest["Size"]

    print(f"Deleting largest object: {key} ({size} bytes)")
    s3.delete_object(Bucket=BUCKET_NAME, Key=key)

    # 同步更新 DynamoDB 里的 total_size
    table.update_item(
        Key={"bucket_name": BUCKET_NAME},
        UpdateExpression="ADD total_size :delta",
        ExpressionAttributeValues={":delta": -size},
    )

    new_size = table.get_item(
        Key={"bucket_name": BUCKET_NAME}
    )["Item"]["total_size"]

    print(f"Deleted {key}. New total size = {new_size} bytes.")