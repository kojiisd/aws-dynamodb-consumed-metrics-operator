import sys
import json

from decimal import Decimal
from time import sleep

import boto3
from boto3.dynamodb.conditions import Key, Attr


args = sys.argv

if len(args) < 6:
    print("Usage: python data-read.py <Region> <Table> <id> <key> <Timing>")
    sys.exit()

dynamodb = boto3.resource('dynamodb', region_name=args[1])
table = dynamodb.Table(args[2])

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

if __name__ == "__main__":
    print("Data read start. For stopping, Ctrl + C press.")
    
    sleep_time = Decimal(1) / Decimal(args[5])
    while True:
      res = table.query(
          KeyConditionExpression=Key('id').eq(args[3])
      )

      return_response = max(res["Items"], key=(lambda x: x[args[4]]))

      print(json.dumps(return_response, default=decimal_default))
      sleep(sleep_time)


