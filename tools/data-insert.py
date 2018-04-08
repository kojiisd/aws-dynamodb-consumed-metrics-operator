import sys

import csv
import math
from decimal import Decimal
from time import sleep
from datetime import datetime
import pandas
import random

import boto3
from boto3.dynamodb.conditions import Key, Attr
from boto3.session import Session


args = sys.argv

if len(args) < 6:
    print("Usage: python data-insert.py <FileName> <Region> <Table> <Timing> <Fluctuation(%)>")
    sys.exit()

dynamodb = boto3.resource('dynamodb', region_name=args[2])
table = dynamodb.Table(args[3])

if __name__ == "__main__":
    print("Data insert start. For stopping, Ctrl + C press.")
    target = pandas.read_csv(args[1])
    timing = Decimal(str(args[4]))
    fluctuation = float(str(args[5]))

    print("Timing: " + str(timing) + "/sec, Fluctuation: " + str(fluctuation) + "%")
    print(target)

    sleep_time = Decimal(1) / timing

    while True:
      for row_index, row in target.iterrows():
          item_dict = {}
          for col in target:
              if col == "id":
                item_dict[col] = str(row[col])
              else:
                value = Decimal(str(row[col]))
                difference = random.uniform(-1 * fluctuation / 100, fluctuation / 100)
                item_dict[col] = value + round(value * Decimal(difference), 2)
          item_dict["timestamp"] = datetime.now().isoformat()
          print(item_dict)

          table.put_item(Item=item_dict)
          sleep(sleep_time)
    
