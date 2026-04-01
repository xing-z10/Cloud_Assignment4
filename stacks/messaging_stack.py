import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_sns as sns,
    aws_sqs as sqs,
    aws_sns_subscriptions as sns_subs,
    Duration,
)
from constructs import Construct


class MessagingStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        topic: sns.Topic,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # ─────────────────────────────────────────
        # SQS Queues
        # ─────────────────────────────────────────
        self.size_tracker_queue = sqs.Queue(
            self, "SizeTrackerQueue",
            visibility_timeout=Duration.seconds(60),
            retention_period=Duration.days(1),
        )

        self.logger_queue = sqs.Queue(
            self, "LoggerQueue",
            visibility_timeout=Duration.seconds(60),
            retention_period=Duration.days(1),
        )

        # ─────────────────────────────────────────
        # SNS → SQS 订阅
        # ─────────────────────────────────────────
        topic.add_subscription(
            sns_subs.SqsSubscription(self.size_tracker_queue)
        )
        topic.add_subscription(
            sns_subs.SqsSubscription(self.logger_queue)
        )

        # ─────────────────────────────────────────
        # Outputs
        # ─────────────────────────────────────────
        cdk.CfnOutput(self, "SizeTrackerQueueUrl", value=self.size_tracker_queue.queue_url)
        cdk.CfnOutput(self, "LoggerQueueUrl",      value=self.logger_queue.queue_url)