import os
from datetime import datetime
from uuid import uuid4

import boto3

KEY_SEPERATOR = '#'


table_name = os.environ["DYNAMODB_TABLE_ARN"]
queue_url = os.environ["SQS_QUEUE_URL"]
dynamodb = boto3.client("dynamodb")
sqs = boto3.client('sqs')


def build_key(*args):
    return KEY_SEPERATOR.join(args)


def rotate_current_event(active_market: dict):
    rotate_event_at = None if active_market["CurrentEventRotate"].get("NULL", False) else active_market["CurrentEventRotate"]["S"]

    if rotate_event_at is not None and datetime.now() < datetime.fromisoformat(rotate_event_at):
        print("It's not time to rotate the current event yet")
        return

    print("Time to rotate the current event")

    sqs.send_message(
        QueueUrl=queue_url,
        MessageAttributes={
            'MarketUUID': {
                'DataType': 'String',
                'StringValue': active_market['UUID']["S"]
            }
        },
        MessageBody='RotateEvent',
        MessageGroupId='FreeMarketFandango',
        MessageDeduplicationId=str(uuid4())
    )


def rotate_stock_prices(active_market: dict):
    print("Checking for price changes")

    response = dynamodb.query(
        TableName=table_name,
        KeyConditionExpression="#PK = :PK and begins_with(#SK, :SK)",
        ExpressionAttributeNames={
            "#PK": "PK",
            "#SK": "SK",
        },
        ExpressionAttributeValues={
            ":PK": {
                "S": build_key("Market", active_market['UUID']["S"])
            },
            ":SK": {
                "S": "Stock"
            },
        }
    )

    for stock in response["Items"]:
        rotate_stock_price_at = stock["PriceRotate"]["S"]
        _, stock_code = stock['SK']["S"].split(KEY_SEPERATOR)

        print(f"Checking {stock_code}")

        if datetime.now() > datetime.fromisoformat(rotate_stock_price_at):
            print(f"{stock_code} is due for a price change")

            sqs.send_message(
                QueueUrl=queue_url,
                MessageAttributes={
                    'MarketUUID': {
                        'DataType': 'String',
                        'StringValue': active_market['UUID']["S"]
                    },
                    'StockCode': {
                        'DataType': 'String',
                        'StringValue': stock_code
                    }
                },
                MessageBody='RotatePrice',
                MessageGroupId='FreeMarketFandango',
                MessageDeduplicationId=str(uuid4())
            )


def handler(event, context):
    active_market_result = dynamodb.get_item(
        TableName=table_name,
        Key={
            "PK": {
                "S": "Market"
            },
            "SK": {
                "S": "Active"
            },
        }
    )

    if "Item" not in active_market_result:
        print("No market has ever been active, will not continue")
        return

    active_market = active_market_result["Item"]
    market_closed_at = active_market["ClosedAt"]

    if "S" in market_closed_at and datetime.now() > datetime.fromisoformat(market_closed_at["S"]):
        print("Market is closed, will not continue")
        return

    rotate_current_event(active_market)
    rotate_stock_prices(active_market)
