import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_logs as logs,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
    Duration,
)
from constructs import Construct


class MonitoringStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        cleaner_fn: _lambda.Function,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # log group 名字与 lambda_stack 里的 function_name 保持一致
        log_group_name = "/aws/lambda/Assignment4-LoggingFunction"

        # ─────────────────────────────────────────
        # 1. Log Group
        # ─────────────────────────────────────────
        log_group = logs.LogGroup(
            self, "LoggingFunctionLogGroup",
            log_group_name=log_group_name,
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # ─────────────────────────────────────────
        # 2. Metric Filter
        # 从日志里提取 size_delta 字段，发布到自定义 metric
        #
        # 日志格式: {"object_name": "xxx.txt", "size_delta": 98}
        # filterPattern 匹配 JSON 日志中存在 size_delta 字段的行
        # metricValue 提取该字段的值（正数=创建，负数=删除）
        # ─────────────────────────────────────────
        metric_filter = logs.MetricFilter(
            self, "SizeDeltaMetricFilter",
            log_group=log_group,
            filter_pattern=logs.FilterPattern.exists("$.size_delta"),
            metric_namespace="Assignment4App",
            metric_name="TotalObjectSize",
            metric_value="$.size_delta",
            default_value=0,
        )

        # 从 metric filter 拿到 metric 对象，供 alarm 使用
        total_size_metric = metric_filter.metric(
            period=Duration.seconds(10),
            statistic="Sum",
        )

        # ─────────────────────────────────────────
        # 3. CloudWatch Alarm
        # SUM(TotalObjectSize) > 20 时触发
        #
        # evaluation_periods=1: 只看最近 1 个统计周期
        # datapoints_to_alarm=1: 1 个周期超标就触发
        # treat_missing_data=NOT_BREACHING: 没有数据时不触发
        # ─────────────────────────────────────────
        alarm = cloudwatch.Alarm(
            self, "TotalSizeAlarm",
            metric=total_size_metric,
            threshold=20,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
            alarm_description="Total object size in bucket exceeded 20 bytes",
        )

        # ─────────────────────────────────────────
        # 4. Alarm Action: Alarm → SNS → Cleaner Lambda
        # CloudWatch Alarm 不能直接触发 Lambda，需要通过 SNS 中转
        # ─────────────────────────────────────────
        alarm_topic = sns.Topic(self, "AlarmTopic")

        # SNS → Cleaner Lambda
        alarm_topic.add_subscription(
            sns_subs.LambdaSubscription(cleaner_fn)
        )

        # Alarm → SNS（alarm 状态变为 ALARM 时触发）
        alarm.add_alarm_action(
            cw_actions.SnsAction(alarm_topic)
        )

        # ─────────────────────────────────────────
        # Outputs
        # ─────────────────────────────────────────
        cdk.CfnOutput(self, "LogGroupName",  value=log_group.log_group_name)
        cdk.CfnOutput(self, "AlarmName",     value=alarm.alarm_name)
        cdk.CfnOutput(self, "AlarmTopicArn", value=alarm_topic.topic_arn)