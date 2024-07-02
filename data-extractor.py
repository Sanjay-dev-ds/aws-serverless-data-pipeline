import json
import boto3
import urllib3
import datetime
import os

FIREHOSE_NAME = os.environ['FIREHOSE_NAME']


def lambda_handler(event, context):
    http = urllib3.PoolManager()

    r = http.request("POST", "https://www.cse.lk/api/aspiData")

    # turn it into a dictionary
    r_dict = json.loads(r.data.decode(encoding='utf-8', errors='strict'))

    # extract pieces of the dictionary
    processed_dict = {}
    processed_dict['id'] = r_dict['id']
    processed_dict['aspi_value'] = r_dict['value']
    processed_dict['lowValue'] = r_dict['lowValue']
    processed_dict['highValue'] = r_dict['highValue']
    processed_dict['change'] = r_dict['change']
    processed_dict['sectorId'] = r_dict['sectorId']
    processed_dict['timestamp'] = r_dict['timestamp']

    # turn it into a string and add a newline
    msg = str(processed_dict) + '\n'

    fh = boto3.client('firehose')

    reply = fh.put_record(
        DeliveryStreamName=FIREHOSE_NAME,
        Record={
            'Data': msg
        }
    )

    return msg
