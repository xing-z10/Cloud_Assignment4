import json
import os
import boto3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, timezone
from io import BytesIO

dynamodb = boto3.resource('dynamodb')
s3       = boto3.client('s3')

TABLE_NAME  = os.environ["TABLE_NAME"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
PLOT_KEY    = "plot.png"


def lambda_handler(event, context):
    try:
        query_params  = event.get('queryStringParameters') or {}
        target_bucket = query_params.get('bucket', BUCKET_NAME)

        plot_buffer = generate_plot(target_bucket)

        # 保存图片到 S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=PLOT_KEY,
            Body=plot_buffer.getvalue(),
            ContentType='image/png',
        )
        print(f"图片已保存至 s3://{BUCKET_NAME}/{PLOT_KEY}")

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
            },
            'body': json.dumps({
                'message':       '图表生成成功',
                'bucket':        BUCKET_NAME,
                'key':           PLOT_KEY,
                'target_bucket': target_bucket,
            }),
        }

    except Exception as e:
        import traceback
        print(f"错误: {str(e)}")
        print(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
            },
            'body': json.dumps({'error': str(e)}),
        }

# handler 别名，兼容 CDK 里配置的 handler="handler.handler"
handler = lambda_handler


def generate_plot(target_bucket):
    table = dynamodb.Table(TABLE_NAME)

    end_time   = datetime.utcnow()
    start_time = end_time - timedelta(seconds=120)  # 最近 2 分钟

    # ── 1. 查询时间窗口内的数据 ──────────────────────────────────────────────
    response = table.query(
        KeyConditionExpression='bucket_name = :bn AND #ts BETWEEN :start AND :end',
        ExpressionAttributeNames={'#ts': 'timestamp'},
        ExpressionAttributeValues={
            ':bn':    target_bucket,
            ':start': start_time.isoformat(),
            ':end':   end_time.isoformat(),
        },
        ScanIndexForward=True,
    )
    items = response.get('Items', [])
    print(f"Found {len(items)} items in last 10 seconds")

    # ── 2. 获取时间窗口之前的最后一条记录（用于填充起始点） ─────────────────
    pre_response = table.query(
        KeyConditionExpression='bucket_name = :bn AND #ts < :start',
        ExpressionAttributeNames={'#ts': 'timestamp'},
        ExpressionAttributeValues={
            ':bn':    target_bucket,
            ':start': start_time.isoformat(),
        },
        ScanIndexForward=False,
        Limit=1,
    )
    pre_items = pre_response.get('Items', [])
    pre_size  = float(pre_items[0]['size_bytes']) if pre_items else 0.0

    # ── 3. 构建时间戳和大小列表 ─────────────────────────────────────────────
    timestamps = []
    sizes      = []
    for item in items:
        ts_str = item.get('timestamp')
        size   = item.get('size_bytes', 0)
        if ts_str:
            timestamps.append(datetime.fromisoformat(ts_str.replace('Z', '+00:00')))
            sizes.append(float(size))
            print(f"  {ts_str}: {size} bytes")

    # ── 4. 填充起始点 ───────────────────────────────────────────────────────
    start_dt = start_time.replace(tzinfo=timezone.utc)
    end_dt   = end_time.replace(tzinfo=timezone.utc)

    if not timestamps or timestamps[0] > start_dt:
        timestamps.insert(0, start_dt)
        sizes.insert(0, pre_size)
        print(f"  Filled start point: {start_dt} -> {pre_size} bytes")

    # ── 5. 填充结束点 ───────────────────────────────────────────────────────
    if not timestamps or timestamps[-1] < end_dt:
        last_size = sizes[-1] if sizes else pre_size
        timestamps.append(end_dt)
        sizes.append(last_size)
        print(f"  Filled end point: {end_dt} -> {last_size} bytes")

    # ── 6. 获取历史最大值 ──────────────────────────────────────────────────
    max_size_ever = get_global_max_size(table)
    print(f"Global max size ever: {max_size_ever}")

    # ── 7. 绘图 ────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(12, 6))

    ax.plot(timestamps, sizes, 'b-', linewidth=2,
            label=f'{target_bucket} Size (Last 10min)', marker='o', markersize=8)

    if max_size_ever > 0:
        ax.axhline(y=max_size_ever, color='r', linestyle='--', linewidth=2,
                   label=f'Max Size Ever: {max_size_ever:,.0f} bytes')

    # 标记时间窗口起止
    ax.axvline(x=start_dt, color='gray', linestyle='--', linewidth=1.5,
               label=f'Start: {start_dt.strftime("%H:%M:%S")} UTC')
    ax.axvline(x=end_dt, color='gray', linestyle='--', linewidth=1.5,
               label=f'End: {end_dt.strftime("%H:%M:%S")} UTC')

    ax.set_xlabel('Timestamp (UTC)', fontsize=12)
    ax.set_ylabel('Size (bytes)', fontsize=12)
    ax.set_title(f'Bucket Size History - {target_bucket}\n(Last 5 minutes)', fontsize=14)
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()

    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
    buffer.seek(0)
    plt.close()

    return buffer


def get_global_max_size(table):
    """获取所有记录中的历史最大值。"""
    max_size = 0.0
    try:
        max_size = max(max_size, get_bucket_max_size(table, BUCKET_NAME))
    except Exception as e:
        print(f"查询最大值出错: {e}")
    return max_size


def get_bucket_max_size(table, bucket_name):
    """扫描指定 bucket 的所有记录，返回最大 size_bytes。"""
    max_size = 0.0
    response = table.query(
        KeyConditionExpression='bucket_name = :bn',
        ExpressionAttributeValues={':bn': bucket_name},
    )
    for item in response.get('Items', []):
        max_size = max(max_size, float(item.get('size_bytes', 0)))

    # 处理分页
    while 'LastEvaluatedKey' in response:
        response = table.query(
            KeyConditionExpression='bucket_name = :bn',
            ExpressionAttributeValues={':bn': bucket_name},
            ExclusiveStartKey=response['LastEvaluatedKey'],
        )
        for item in response.get('Items', []):
            max_size = max(max_size, float(item.get('size_bytes', 0)))

    return max_size