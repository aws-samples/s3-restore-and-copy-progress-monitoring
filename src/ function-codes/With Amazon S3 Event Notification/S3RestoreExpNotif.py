import json
import boto3
import datetime
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import logging
import os

dynamodb = boto3.resource('dynamodb')
logger = logging.getLogger(__name__)
print('Loading function')
versioned = os.environ['versioned']
ddbtable = os.environ['TableName']


def lambda_handler(event, context):
  if 'versionId' not in event['Records'][0]['s3']['object'] and versioned == 'no':
    eventtime = event['Records'][0]['eventTime']
    eventtype = event['Records'][0]['eventName']
    bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    #size = event['Records'][0]['s3']['object']['size']
    #version = event['Records'][0]['s3']['object']['versionId']
    table = dynamodb.Table(ddbtable)

    get_response = table.get_item(
        Key={ 'object_key': object_key },
        )
        
    if len(get_response) == 1:
        print("object does not exist")
    else:
        restore_finishtime = datetime.datetime.strptime(get_response['Item']['restore_init_time'], '%Y-%m-%dT%H:%M:%S.%fZ')
        expiration_time = datetime.datetime.strptime(eventtime, '%Y-%m-%dT%H:%M:%S.%fZ')
        expiration_delta = expiration_time - restore_finishtime
        expiration_duration = expiration_delta.total_seconds()/60/60

    try:
        update_response =  table.update_item(
            Key={ 'object_key': object_key },
            UpdateExpression="set object_accessible = :val1, expiration_time = :val2, expiration_duration = :val3, object_expired = :val4",
            ExpressionAttributeValues={':val1': "no", ':val2': eventtime, ':val3': int(expiration_duration), ':val4': "yes"},
            ConditionExpression='attribute_exists(object_key)',
            ReturnValues="UPDATED_NEW")
    except ClientError as err:
        logger.warning(
            "something went wrong")
  
  elif 'versionId' in event['Records'][0]['s3']['object'] and versioned == 'no':
    eventtime = event['Records'][0]['eventTime']
    eventtype = event['Records'][0]['eventName']
    bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    #size = event['Records'][0]['s3']['object']['size']
    #version = event['Records'][0]['s3']['object']['versionId']
    table = dynamodb.Table(ddbtable)

    get_response = table.get_item(
        Key={ 'object_key': object_key },
        )
        
    if len(get_response) == 1:
        print("object does not exist")
    else:
        restore_finishtime = datetime.datetime.strptime(get_response['Item']['restore_init_time'], '%Y-%m-%dT%H:%M:%S.%fZ')
        expiration_time = datetime.datetime.strptime(eventtime, '%Y-%m-%dT%H:%M:%S.%fZ')
        expiration_delta = expiration_time - restore_finishtime
        expiration_duration = expiration_delta.total_seconds()/60/60

    try:
        update_response =  table.update_item(
            Key={ 'object_key': object_key },
            UpdateExpression="set object_accessible = :val1, expiration_time = :val2, expiration_duration = :val3, object_expired = :val4",
            ExpressionAttributeValues={':val1': "no", ':val2': eventtime, ':val3': int(expiration_duration), ':val4': "yes"},
            ConditionExpression='attribute_exists(object_key)',
            ReturnValues="UPDATED_NEW")
    except ClientError as err:
        logger.warning(
            "something went wrong")

  elif 'versionId' not in event['Records'][0]['s3']['object'] and versioned == 'yes':
    eventtime = event['Records'][0]['eventTime']
    eventtype = event['Records'][0]['eventName']
    bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    #size = event['Records'][0]['s3']['object']['size']
    versionId = "null"
    table = dynamodb.Table(ddbtable)

    get_response = table.get_item(
        Key={ 'object_key': object_key, 'versionId': versionId},
        )
        
    if len(get_response) == 1:
        print("object does not exist")
    else:
        restore_finishtime = datetime.datetime.strptime(get_response['Item']['restore_init_time'], '%Y-%m-%dT%H:%M:%S.%fZ')
        expiration_time = datetime.datetime.strptime(eventtime, '%Y-%m-%dT%H:%M:%S.%fZ')
        expiration_delta = expiration_time - restore_finishtime
        expiration_duration = expiration_delta.total_seconds()/60/60

    try:
        update_response =  table.update_item(
            Key={ 'object_key': object_key, 'versionId': versionId },
            UpdateExpression="set object_accessible = :val1, expiration_time = :val2, expiration_duration = :val3, object_expired = :val4",
            ExpressionAttributeValues={':val1': "no", ':val2': eventtime, ':val3': int(expiration_duration), ':val4': "yes"},
            ConditionExpression='attribute_exists(object_key) and attribute_exists(versionId)',
            ReturnValues="UPDATED_NEW")
    except ClientError as err:
        logger.warning(
            "something went wrong")

  elif 'versionId' in event['Records'][0]['s3']['object'] and versioned == 'yes':
    eventtime = event['Records'][0]['eventTime']
    eventtype = event['Records'][0]['eventName']
    bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    #size = event['Records'][0]['s3']['object']['size']
    versionId = event['Records'][0]['s3']['object']['versionId']
    table = dynamodb.Table(ddbtable)

    get_response = table.get_item(
        Key={ 'object_key': object_key, 'versionId': versionId},
        )
        
    if len(get_response) == 1:
        print("object does not exist")
    else:
        restore_finishtime = datetime.datetime.strptime(get_response['Item']['restore_init_time'], '%Y-%m-%dT%H:%M:%S.%fZ')
        expiration_time = datetime.datetime.strptime(eventtime, '%Y-%m-%dT%H:%M:%S.%fZ')
        expiration_delta = expiration_time - restore_finishtime
        expiration_duration = expiration_delta.total_seconds()/60/60

    try:
        update_response =  table.update_item(
            Key={ 'object_key': object_key, 'versionId': versionId },
            UpdateExpression="set object_accessible = :val1, expiration_time = :val2, expiration_duration = :val3, object_expired = :val4",
            ExpressionAttributeValues={':val1': "no", ':val2': eventtime, ':val3': int(expiration_duration), ':val4': "yes"},
            ConditionExpression='attribute_exists(object_key) and attribute_exists(versionId)',
            ReturnValues="UPDATED_NEW")
    except ClientError as err:
        logger.warning(
            "something went wrong")

