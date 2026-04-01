import aws_cdk as cdk

from stacks.storage_stack import StorageStack
from stacks.messaging_stack import MessagingStack
from stacks.lambda_stack import LambdaStack
from stacks.monitoring_stack import MonitoringStack

app = cdk.App()

# 1. 存储层：S3 bucket + DynamoDB table
storage = StorageStack(app, "StorageStack")

# 2. 消息层：SQS queues + SNS 订阅（topic 已在 StorageStack 里创建）
messaging = MessagingStack(
    app, "MessagingStack",
    topic=storage.topic,
)

# 3. 计算层：所有 Lambda + API Gateway
lambdas = LambdaStack(
    app, "LambdaStack",
    bucket=storage.bucket,
    table=storage.table,
    size_tracker_queue=messaging.size_tracker_queue,
    logger_queue=messaging.logger_queue,
)

# 4. 监控层：Log group + Metric filter + Alarm + Cleaner 触发
monitoring = MonitoringStack(
    app, "MonitoringStack",
    cleaner_fn=lambdas.cleaner_fn,
)

app.synth()