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
  if 'version-id' not in event['detail']['object'] and versioned == 'no':
    bucket = event['detail']['bucket']['name']
    object_key = event['detail']['object']['key']
    size = event['detail']['object']['size']
    eventtime = event['time']
    table = dynamodb.Table(ddbtable)

    get_response = table.get_item(
        Key={ 'object_key': object_key },
        )
        
    if len(get_response) == 1:
        print("object does not exist")
    else:
        restore_starttime = datetime.datetime.strptime(get_response['Item']['restore_init_time'], '%Y-%m-%dT%H:%M:%S%fZ')
        restore_finishtime = datetime.datetime.strptime(eventtime, '%Y-%m-%dT%H:%M:%S%fZ')
        restore_delta = restore_finishtime - restore_starttime
        restore_duration = restore_delta.total_seconds()/60

    try:
        update_response =  table.update_item(
            Key={ 'object_key': object_key },
            UpdateExpression="set restore_completed = :s, restore_comp_time = :c, restore_duration = :e, object_accessible = :val1",
            ExpressionAttributeValues={':s': "yes", ':c': eventtime, ':e': int(restore_duration), ':val1': "yes"},
            ConditionExpression='attribute_exists(object_key)',
            ReturnValues="UPDATED_NEW")
    except ClientError as err:
        logger.warning(
            "something went wrong")

  elif 'version-id' in event['detail']['object'] and versioned == 'no':
    bucket = event['detail']['bucket']['name']
    object_key = event['detail']['object']['key']
    size = event['detail']['object']['size']
    eventtime = event['time']
    table = dynamodb.Table(ddbtable)

    get_response = table.get_item(
        Key={ 'object_key': object_key },
        )
        
    if len(get_response) == 1:
        print("object does not exist")
    else:
        restore_starttime = datetime.datetime.strptime(get_response['Item']['restore_init_time'], '%Y-%m-%dT%H:%M:%S%fZ')
        restore_finishtime = datetime.datetime.strptime(eventtime, '%Y-%m-%dT%H:%M:%S%fZ')
        restore_delta = restore_finishtime - restore_starttime
        restore_duration = restore_delta.total_seconds()/60

    try:
        update_response =  table.update_item(
            Key={ 'object_key': object_key },
            UpdateExpression="set restore_completed = :s, restore_comp_time = :c, restore_duration = :e, object_accessible = :val1",
            ExpressionAttributeValues={':s': "yes", ':c': eventtime, ':e': int(restore_duration), ':val1': "yes"},
            ConditionExpression='attribute_exists(object_key)',
            ReturnValues="UPDATED_NEW")
    except ClientError as err:
        logger.warning(
            "something went wrong")

  elif 'version-id' not in event['detail']['object'] and versioned == 'yes':
    bucket = event['detail']['bucket']['name']
    object_key = event['detail']['object']['key']
    size = event['detail']['object']['size']
    versionId = "null"
    eventtime = event['time']  
    table = dynamodb.Table(ddbtable)

    get_response = table.get_item(
        Key={ 'object_key': object_key, 'versionId': versionId},
        )
        
    if len(get_response) == 1:
        print("object does not exist")
    else:
        restore_starttime = datetime.datetime.strptime(get_response['Item']['restore_init_time'], '%Y-%m-%dT%H:%M:%S%fZ')
        restore_finishtime = datetime.datetime.strptime(eventtime, '%Y-%m-%dT%H:%M:%S%fZ')
        restore_delta = restore_finishtime - restore_starttime
        restore_duration = restore_delta.total_seconds()/60

    try:
        update_response =  table.update_item(
            Key={ 'object_key': object_key, 'versionId': versionId},
            UpdateExpression="set restore_completed = :s, restore_comp_time = :c, restore_duration = :e, object_accessible = :val1",
            ExpressionAttributeValues={':s': "yes", ':c': eventtime, ':e': int(restore_duration), ':val1': "yes"},
            ConditionExpression='attribute_exists(object_key) and attribute_exists(versionId)',
            ReturnValues="UPDATED_NEW")
    except ClientError as err:
        logger.warning(
            "something went wrong")

  elif 'version-id' in event['detail']['object'] and versioned == 'yes':
    bucket = event['detail']['bucket']['name']
    object_key = event['detail']['object']['key']
    size = event['detail']['object']['size']
    versionId = event['detail']['object']['version-id']
    eventtime = event['time']
    table = dynamodb.Table(ddbtable)

    get_response = table.get_item(
        Key={ 'object_key': object_key, 'versionId': versionId},
        )
        
    if len(get_response) == 1:
        print("object does not exist")
    else:
        restore_starttime = datetime.datetime.strptime(get_response['Item']['restore_init_time'], '%Y-%m-%dT%H:%M:%S%fZ')
        restore_finishtime = datetime.datetime.strptime(eventtime, '%Y-%m-%dT%H:%M:%S%fZ')
        restore_delta = restore_finishtime - restore_starttime
        restore_duration = restore_delta.total_seconds()/60

    try:
        update_response =  table.update_item(
            Key={ 'object_key': object_key, 'versionId': versionId},
            UpdateExpression="set restore_completed = :s, restore_comp_time = :c, restore_duration = :e, object_accessible = :val1",
            ExpressionAttributeValues={':s': "yes", ':c': eventtime, ':e': int(restore_duration), ':val1': "yes"},
            ConditionExpression='attribute_exists(object_key) and attribute_exists(versionId)',
            ReturnValues="UPDATED_NEW")
    except ClientError as err:
        logger.warning(
            "something went wrong")
