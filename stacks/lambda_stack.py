import os
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_events,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_iam as iam,
    aws_apigateway as apigw,
    Duration,
)
from constructs import Construct


def lambda_code(name: str) -> _lambda.Code:
    """
    从 lambdas/<name>/ 目录构建 Lambda 代码。
    若目录下存在 requirements.txt，则自动 pip install 打包依赖；
    否则直接使用源码目录。
    """
    asset_path = os.path.join("lambdas", name)
    req_file   = os.path.join(asset_path, "requirements.txt")

    if os.path.exists(req_file):
        return _lambda.Code.from_asset(
            asset_path,
            bundling=cdk.BundlingOptions(
                image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                command=[
                    "bash", "-c",
                    "pip install -r requirements.txt -t /asset-output && cp -r . /asset-output",
                ],
            ),
        )
    return _lambda.Code.from_asset(asset_path)


class LambdaStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        bucket: s3.Bucket,
        table: dynamodb.Table,
        size_tracker_queue: sqs.Queue,
        logger_queue: sqs.Queue,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # ─────────────────────────────────────────
        # matplotlib Layer (ARM64)
        # 预先在本地打包好，放在 layers/matplotlib/ 目录下
        # 打包命令：
        #   mkdir -p layers/matplotlib/python
        #   pip install matplotlib -t layers/matplotlib/python \
        #       --platform manylinux2014_aarch64 \
        #       --implementation cp \
        #       --python-version 3.12 \
        #       --only-binary=:all:
        # ─────────────────────────────────────────
        matplotlib_layer = _lambda.LayerVersion(
            self, "MatplotlibLayer",
            code=_lambda.Code.from_asset("layers/matplotlib"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            compatible_architectures=[_lambda.Architecture.ARM_64],
            description="matplotlib for ARM64 Python 3.12",
        )

        # log group 名字固定，避免跨 Stack 引用导致循环依赖
        logging_fn_name = "Assignment4-LoggingFunction"

        common_env = {
            "BUCKET_NAME":        bucket.bucket_name,
            "TABLE_NAME":         table.table_name,
            "LOGGING_FN_NAME":    logging_fn_name,
        }

        # ─────────────────────────────────────────
        # 1. Size Tracker Lambda
        # 从 SizeTrackerQueue 消费，更新 DynamoDB 里的 bucket 总大小
        # ─────────────────────────────────────────
        self.size_tracker_fn = _lambda.Function(
            self, "SizeTrackerFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_code("size_tracker"),
            environment=common_env,
            timeout=Duration.seconds(30),
        )
        self.size_tracker_fn.add_event_source(
            lambda_events.SqsEventSource(size_tracker_queue, batch_size=1)
        )
        table.grant_read_write_data(self.size_tracker_fn)

        # ─────────────────────────────────────────
        # 2. Logging Lambda
        # 从 LoggerQueue 消费，写 JSON 日志到 CloudWatch
        # 删除事件时需要查询自己的日志组获取对象大小
        # ─────────────────────────────────────────
        self.logging_fn = _lambda.Function(
            self, "LoggingFunction",
            function_name=logging_fn_name,
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_code("logging_lambda"),
            environment=common_env,
            timeout=Duration.seconds(30),
        )
        self.logging_fn.add_event_source(
            lambda_events.SqsEventSource(logger_queue, batch_size=1)
        )
        # 允许 logging lambda 查询自己的日志组（用于删除事件查找对象大小）
        # 直接用固定的 function_name 字符串，避免循环引用
        self.logging_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["logs:FilterLogEvents"],
            resources=[
                f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/lambda/{logging_fn_name}:*"
            ],
        ))

        # ─────────────────────────────────────────
        # 3. Cleaner Lambda
        # 由 CloudWatch Alarm 触发（通过 SNS），删除 bucket 中最大的对象
        # ─────────────────────────────────────────
        self.cleaner_fn = _lambda.Function(
            self, "CleanerFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_code("cleaner"),
            environment=common_env,
            timeout=Duration.seconds(30),
        )
        bucket.grant_read(self.cleaner_fn)       # 需要 list + get 来找最大对象
        bucket.grant_delete(self.cleaner_fn)     # 需要 delete

        # ─────────────────────────────────────────
        # 4. Driver Lambda
        # 按顺序创建对象，最后调用 plotter API
        # ─────────────────────────────────────────
        self.driver_fn = _lambda.Function(
            self, "DriverFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_code("driver"),
            environment=common_env,
            timeout=Duration.seconds(300),   # driver 需要 sleep，timeout 设长一些
        )
        bucket.grant_put(self.driver_fn)

        # ─────────────────────────────────────────
        # 5. Plotter Lambda + API Gateway
        # 读取 DynamoDB，生成 size 变化图表
        # ─────────────────────────────────────────
        self.plotter_fn = _lambda.Function(
            self, "PlotterFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            code=lambda_code("plotter"),
            environment=common_env,
            timeout=Duration.seconds(30),
            layers=[matplotlib_layer],
        )
        table.grant_read_data(self.plotter_fn)
        bucket.grant_put(self.plotter_fn)
        self.plotter_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["cloudwatch:GetMetricStatistics"],
            resources=["*"],
        ))

        api = apigw.LambdaRestApi(
            self, "PlotterApi",
            handler=self.plotter_fn,
            proxy=True,
        )

        # API URL 创建后再注入给 driver
        self.driver_fn.add_environment("PLOTTER_API_URL", api.url)

        # ─────────────────────────────────────────
        # Outputs
        # ─────────────────────────────────────────
        cdk.CfnOutput(self, "PlotterApiUrl",       value=api.url)
        cdk.CfnOutput(self, "DriverFunctionName",  value=self.driver_fn.function_name)
        cdk.CfnOutput(self, "CleanerFunctionName", value=self.cleaner_fn.function_name)
        cdk.CfnOutput(self, "LoggingFunctionName", value=self.logging_fn.function_name)