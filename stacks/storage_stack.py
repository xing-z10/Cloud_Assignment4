import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_sns as sns,
    aws_s3_notifications as s3n,
    RemovalPolicy,
)
from constructs import Construct


class StorageStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ─────────────────────────────────────────
        # S3 Bucket
        # ─────────────────────────────────────────
        self.bucket = s3.Bucket(
            self, "TestBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # ─────────────────────────────────────────
        # DynamoDB Table
        # ─────────────────────────────────────────
        self.table = dynamodb.Table(
            self, "SizeTable",
            partition_key=dynamodb.Attribute(
                name="bucket_name",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ─────────────────────────────────────────
        # SNS Topic (放在这里避免跨 Stack 循环依赖)
        # S3 event notification 必须和 bucket 在同一个 Stack
        # ─────────────────────────────────────────
        self.topic = sns.Topic(self, "S3EventsTopic")

        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SnsDestination(self.topic),
        )
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_REMOVED,
            s3n.SnsDestination(self.topic),
        )

        # ─────────────────────────────────────────
        # Outputs
        # ─────────────────────────────────────────
        cdk.CfnOutput(self, "BucketName", value=self.bucket.bucket_name)
        cdk.CfnOutput(self, "TableName",  value=self.table.table_name)
        cdk.CfnOutput(self, "TopicArn",   value=self.topic.topic_arn)