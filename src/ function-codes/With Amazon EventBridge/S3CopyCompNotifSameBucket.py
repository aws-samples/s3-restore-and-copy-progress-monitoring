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

print(datetime.datetime.utcnow())

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

        copy_finishtime = datetime.datetime.strptime(eventtime, '%Y-%m-%dT%H:%M:%S%fZ')
        
        restore_finishtime = datetime.datetime.strptime(get_response['Item']['restore_comp_time'], '%Y-%m-%dT%H:%M:%S%fZ')
        copy_restore_delta = copy_finishtime - restore_finishtime
        duration_in_std = copy_restore_delta.total_seconds()/60
    
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key },
                UpdateExpression="set copy_initiated = :s, copy_completed = :cm, copy_comp_time = :cc, duration_in_std = :ds, object_accessible = :val1",
                ExpressionAttributeValues={':s': "yes", ':cm': "yes",':cc': eventtime, ':ds': int(duration_in_std), ':val1': "no"},
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

        copy_finishtime = datetime.datetime.strptime(eventtime, '%Y-%m-%dT%H:%M:%S%fZ')
        
        restore_finishtime = datetime.datetime.strptime(get_response['Item']['restore_comp_time'], '%Y-%m-%dT%H:%M:%S%fZ')
        copy_restore_delta = copy_finishtime - restore_finishtime
        duration_in_std = copy_restore_delta.total_seconds()/60
    
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key },
                UpdateExpression="set copy_initiated = :s, copy_completed = :cm, copy_comp_time = :cc, duration_in_std = :ds, object_accessible = :val1",
                ExpressionAttributeValues={':s': "yes", ':cm': "yes",':cc': eventtime, ':ds': int(duration_in_std), ':val1': "no"},
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

    response = table.query(
      KeyConditionExpression=Key('object_key').eq(object_key)
    )
    
    if len(response['Items']) > 0:
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key , 'versionId': versionId},
                UpdateExpression="set copy_initiated = :s, copy_completed = :cm, copy_comp_time = :cc, object_accessible = :val1, new_copy = :val2",
                ExpressionAttributeValues={':s': "yes", ':cm': "yes",':cc': eventtime, ':val1': "yes", ':val2': "yes"},
                ConditionExpression='attribute_exists(object_key)',
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.warning(
                "something went wrong")  


    #     try:
    #         update_response =  table.put_item(
    #             Item={ 'bucket': bucket, 'object_key': object_key, 'versionId': versionId, 'new_copy': "yes", 'copy_completed': "yes", 'copy_comp_time': eventtime}
    #             )
    #     except ClientError as err:
    #         logger.warning(
    #             "something went wrong")
    # else:
    #     print("object created out of scope")

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

    response = table.query(
      KeyConditionExpression=Key('object_key').eq(object_key)
    )
    
    if len(response['Items']) > 0:
        try:
            update_response =  table.put_item(
                Item={ 'bucket': bucket, 'object_key': object_key, 'versionId': versionId, 'new_copy': "yes", 'copy_completed': "yes", 'copy_comp_time': eventtime}
                )
        except ClientError as err:
            logger.warning(
                "something went wrong")
    else:
        print("object created out of scope")
