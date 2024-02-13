import json
import boto3
from botocore.exceptions import ClientError
import logging
import os

logger = logging.getLogger(__name__)

print('Loading function')

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

versioned = os.environ['versioned']
ddbtable = os.environ['TableName']

def lambda_handler(event, context):
  if 'versionId' not in event['Records'][0]['s3']['object'] and versioned == 'no':
    bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    size = event['Records'][0]['s3']['object']['size']
    #version = event['Records'][0]['s3']['object']['versionId']
    eventtime = event['Records'][0]['eventTime']
    eventtype = event['Records'][0]['eventName']
    
  
    table = dynamodb.Table(ddbtable)
    
    get_response = table.get_item(
         Key={ 'object_key': object_key },
        )
    if len(get_response) == 1:
        print("object does not exist")
    
    elif 'object_expired' not in get_response['Item']:
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key },
                UpdateExpression="set restore_initiated = :val1, restore_init_time = :val2, size = :val3, restore_comp_time = :val4, copy_comp_time = :val5, restore_duration = :val6, duration_in_std = :val7,restore_completed = :val8, copy_completed = :val9, object_accessible = :val10, expiration_duration = :val11, object_expired = :val12",
                ExpressionAttributeValues={':val1': "yes", ':val2': eventtime, ':val3': size, ':val4': "null", ':val5': "null", ':val6': 0, ':val7': 0, ':val8': "no", ':val9': "no", ':val10': "no", ':val11': 0, ':val12': "no"},
                ConditionExpression='attribute_exists(object_key)',
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.warning(
                "something went wrong")

    elif 'object_expired' in get_response['Item'] and get_response['Item']['object_expired'] == 'yes':
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key },
                UpdateExpression="set restore_initiated = :val1, restore_init_time = :val2, size = :val3, restore_comp_time = :val4, copy_comp_time = :val5, restore_duration = :val6, duration_in_std = :val7,restore_completed = :val8, copy_completed = :val9, object_accessible = :val10, expiration_duration = :val11, object_expired = :val12",
                ExpressionAttributeValues={':val1': "yes", ':val2': eventtime, ':val3': size, ':val4': "null", ':val5': "null", ':val6': 0, ':val7': 0, ':val8': "no", ':val9': "no", ':val10': "no", ':val11': 0, ':val12': "no"},
                ConditionExpression='attribute_exists(object_key)',
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.warning(
                "something went wrong")

    elif 'object_expired' in get_response['Item'] and get_response['Item']['object_accessible'] == 'yes':
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key },
                UpdateExpression="set restore_initiated = :val1, copy_comp_time = :val5, restore_duration = :val6, duration_in_std = :val7,restore_completed = :val8, copy_completed = :val9, object_accessible = :val10, expiration_duration = :val11, object_expired = :val12",
                ExpressionAttributeValues={':val1': "yes", ':val5': "null", ':val6': 0, ':val7': 0, ':val8': "yes", ':val9': "no", ':val10': "yes", ':val11': 0, ':val12': "no"},
                ConditionExpression='attribute_exists(object_key)',
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.warning(
                "something went wrong")


    elif 'object_expired' in get_response['Item'] and get_response['Item']['object_accessible'] == 'no':
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key },
                UpdateExpression="set restore_initiated = :val1, restore_init_time = :val2, size = :val3, restore_comp_time = :val4, copy_comp_time = :val5, duration_in_std = :val7, restore_completed = :val8, copy_completed = :val9, object_accessible = :val10, expiration_duration = :val11, object_expired = :val12",
                ExpressionAttributeValues={':val1': "yes", ':val2': eventtime, ':val3': size, ':val4': "null", ':val5': "null", ':val7': 0, ':val8': "no", ':val9': "no", ':val10': "no", ':val11': 0, ':val12': "no"},
                ConditionExpression='attribute_exists(object_key)',
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.warning(
                "something went wrong")                         
  
  elif 'versionId' in event['Records'][0]['s3']['object'] and versioned == 'no':
    bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    size = event['Records'][0]['s3']['object']['size']
    #version = event['Records'][0]['s3']['object']['versionId']
    eventtime = event['Records'][0]['eventTime']
    eventtype = event['Records'][0]['eventName']
    
  
    table = dynamodb.Table(ddbtable)
    
    get_response = table.get_item(
         Key={ 'object_key': object_key },
        )
    if len(get_response) == 1:
        print("object does not exist")
    
    elif 'object_expired' not in get_response['Item']:
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key },
                UpdateExpression="set restore_initiated = :val1, restore_init_time = :val2, size = :val3, restore_comp_time = :val4, copy_comp_time = :val5, restore_duration = :val6, duration_in_std = :val7,restore_completed = :val8, copy_completed = :val9, object_accessible = :val10, expiration_duration = :val11, object_expired = :val12",
                ExpressionAttributeValues={':val1': "yes", ':val2': eventtime, ':val3': size, ':val4': "null", ':val5': "null", ':val6': 0, ':val7': 0, ':val8': "no", ':val9': "no", ':val10': "no", ':val11': 0, ':val12': "no"},
                ConditionExpression='attribute_exists(object_key)',
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.warning(
                "something went wrong")

    elif 'object_expired' in get_response['Item'] and get_response['Item']['object_expired'] == 'yes':
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key },
                UpdateExpression="set restore_initiated = :val1, restore_init_time = :val2, size = :val3, restore_comp_time = :val4, copy_comp_time = :val5, restore_duration = :val6, duration_in_std = :val7,restore_completed = :val8, copy_completed = :val9, object_accessible = :val10, expiration_duration = :val11, object_expired = :val12",
                ExpressionAttributeValues={':val1': "yes", ':val2': eventtime, ':val3': size, ':val4': "null", ':val5': "null", ':val6': 0, ':val7': 0, ':val8': "no", ':val9': "no", ':val10': "no", ':val11': 0, ':val12': "no"},
                ConditionExpression='attribute_exists(object_key)',
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.warning(
                "something went wrong")

    elif 'object_expired' in get_response['Item'] and get_response['Item']['object_accessible'] == 'yes':
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key },
                UpdateExpression="set restore_initiated = :val1, copy_comp_time = :val5, restore_duration = :val6, duration_in_std = :val7,restore_completed = :val8, copy_completed = :val9, object_accessible = :val10, expiration_duration = :val11, object_expired = :val12",
                ExpressionAttributeValues={':val1': "yes", ':val5': "null", ':val6': 0, ':val7': 0, ':val8': "yes", ':val9': "no", ':val10': "yes", ':val11': 0, ':val12': "no"},
                ConditionExpression='attribute_exists(object_key)',
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.warning(
                "something went wrong")


    elif 'object_expired' in get_response['Item'] and get_response['Item']['object_accessible'] == 'no':
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key },
                UpdateExpression="set restore_initiated = :val1, restore_init_time = :val2, size = :val3, restore_comp_time = :val4, copy_comp_time = :val5, duration_in_std = :val7, restore_completed = :val8, copy_completed = :val9, object_accessible = :val10, expiration_duration = :val11, object_expired = :val12",
                ExpressionAttributeValues={':val1': "yes", ':val2': eventtime, ':val3': size, ':val4': "null", ':val5': "null", ':val7': 0, ':val8': "no", ':val9': "no", ':val10': "no", ':val11': 0, ':val12': "no"},
                ConditionExpression='attribute_exists(object_key)',
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.warning(
                "something went wrong") 

  elif 'versionId' not in event['Records'][0]['s3']['object'] and versioned == 'yes':
    bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    size = event['Records'][0]['s3']['object']['size']
    versionId = "null"
    eventtime = event['Records'][0]['eventTime']
    eventtype = event['Records'][0]['eventName']
    
  
    table = dynamodb.Table(ddbtable)
    
    get_response = table.get_item(
         Key={ 'object_key': object_key, 'versionId': versionId},
        )
    if len(get_response) == 1:
        print("object does not exist")
    
    elif 'object_expired' not in get_response['Item']:
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key, 'versionId': versionId},
                UpdateExpression="set restore_initiated = :val1, restore_init_time = :val2, size = :val3, restore_comp_time = :val4, copy_comp_time = :val5, restore_duration = :val6, duration_in_std = :val7,restore_completed = :val8, copy_completed = :val9, object_accessible = :val10, expiration_duration = :val11, object_expired = :val12, new_copy = :val13",
                ExpressionAttributeValues={':val1': "yes", ':val2': eventtime, ':val3': size, ':val4': "null", ':val5': "null", ':val6': 0, ':val7': 0, ':val8': "no", ':val9': "no", ':val10': "no", ':val11': 0, ':val12': "no", ':val13': "no"},
                ConditionExpression='attribute_exists(object_key) and attribute_exists(versionId)',
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.warning(
                "something went wrong")
    
    elif 'object_expired' in get_response['Item'] and get_response['Item']['object_expired'] == 'yes':
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key, 'versionId': versionId},
                UpdateExpression="set restore_initiated = :val1, restore_init_time = :val2, size = :val3, restore_comp_time = :val4, copy_comp_time = :val5, restore_duration = :val6, duration_in_std = :val7,restore_completed = :val8, copy_completed = :val9, object_accessible = :val10, expiration_duration = :val11, object_expired = :val12, new_copy = :val13",
                ExpressionAttributeValues={':val1': "yes", ':val2': eventtime, ':val3': size, ':val4': "null", ':val5': "null", ':val6': 0, ':val7': 0, ':val8': "no", ':val9': "no", ':val10': "no", ':val11': 0, ':val12': "no", ':val13': "no"},
                ConditionExpression='attribute_exists(object_key) and attribute_exists(versionId)',
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.warning(
                "something went wrong")
    
    elif 'object_expired' in get_response['Item'] and get_response['Item']['object_expired'] == 'no':
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key, 'versionId': versionId},
                UpdateExpression="set restore_initiated = :val1, restore_init_time = :val2, size = :val3, copy_comp_time = :val5, duration_in_std = :val7, copy_completed = :val9, object_accessible = :val10, expiration_duration = :val11, object_expired = :val12, new_copy = :val13",
                ExpressionAttributeValues={':val1': "yes", ':val2': eventtime, ':val3': size, ':val5': "null", ':val7': 0, ':val8': "no", ':val9': "no", ':val10': "yes", ':val11': 0, ':val12': "no", ':val13': "no"},
                ConditionExpression='attribute_exists(object_key) and attribute_exists(versionId)',
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.warning(
                "something went wrong") 

  elif 'versionId' in event['Records'][0]['s3']['object'] and versioned == 'yes':
    bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    size = event['Records'][0]['s3']['object']['size']
    versionId = event['Records'][0]['s3']['object']['versionId']
    eventtime = event['Records'][0]['eventTime']
    eventtype = event['Records'][0]['eventName']
    
  
    table = dynamodb.Table(ddbtable)
    
    get_response = table.get_item(
         Key={ 'object_key': object_key, 'versionId': versionId},
        )
    if len(get_response) == 1:
        print("object does not exist")
    
    elif 'object_expired' not in get_response['Item']:
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key, 'versionId': versionId},
                UpdateExpression="set restore_initiated = :val1, restore_init_time = :val2, size = :val3, restore_comp_time = :val4, copy_comp_time = :val5, restore_duration = :val6, duration_in_std = :val7,restore_completed = :val8, copy_completed = :val9, object_accessible = :val10, expiration_duration = :val11, object_expired = :val12, new_copy = :val13",
                ExpressionAttributeValues={':val1': "yes", ':val2': eventtime, ':val3': size, ':val4': "null", ':val5': "null", ':val6': 0, ':val7': 0, ':val8': "no", ':val9': "no", ':val10': "no", ':val11': 0, ':val12': "no", ':val13': "no"},
                ConditionExpression='attribute_exists(object_key) and attribute_exists(versionId)',
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.warning(
                "something went wrong")
    
    elif 'object_expired' in get_response['Item'] and get_response['Item']['object_expired'] == 'yes':
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key, 'versionId': versionId},
                UpdateExpression="set restore_initiated = :val1, restore_init_time = :val2, size = :val3, restore_comp_time = :val4, copy_comp_time = :val5, restore_duration = :val6, duration_in_std = :val7,restore_completed = :val8, copy_completed = :val9, object_accessible = :val10, expiration_duration = :val11, object_expired = :val12, new_copy = :val13",
                ExpressionAttributeValues={':val1': "yes", ':val2': eventtime, ':val3': size, ':val4': "null", ':val5': "null", ':val6': 0, ':val7': 0, ':val8': "no", ':val9': "no", ':val10': "no", ':val11': 0, ':val12': "no", ':val13': "no"},
                ConditionExpression='attribute_exists(object_key) and attribute_exists(versionId)',
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.warning(
                "something went wrong")
    
    elif 'object_expired' in get_response['Item'] and get_response['Item']['object_expired'] == 'no':
        try:
            update_response =  table.update_item(
                Key={ 'object_key': object_key, 'versionId': versionId},
                UpdateExpression="set restore_initiated = :val1, restore_init_time = :val2, size = :val3, copy_comp_time = :val5, duration_in_std = :val7, copy_completed = :val9, object_accessible = :val10, expiration_duration = :val11, object_expired = :val12, new_copy = :val13",
                ExpressionAttributeValues={':val1': "yes", ':val2': eventtime, ':val3': size, ':val5': "null", ':val7': 0, ':val8': "no", ':val9': "no", ':val10': "yes", ':val11': 0, ':val12': "no", ':val13': "no"},
                ConditionExpression='attribute_exists(object_key) and attribute_exists(versionId)',
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.warning(
                "something went wrong")                           

