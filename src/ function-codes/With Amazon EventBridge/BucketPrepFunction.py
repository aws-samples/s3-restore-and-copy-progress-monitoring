import json
import cfnresponse
import logging
import os
import boto3
from botocore.exceptions import ClientError
from botocore.client import Config

# Enable debugging for troubleshooting
# boto3.set_stream_logger("")


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


# Define Environmental Variables
my_region = str(os.environ['AWS_REGION'])


# Set SDK paramters
config = Config(retries = {'max_attempts': 5})

# Set variables
# Set Service Parameters
s3Client = boto3.client('s3', config=config, region_name=my_region)


def check_bucket_exists(bucket):
    logger.info(f"Checking if Archive Bucket Exists")
    try:
        check_bucket = s3Client.get_bucket_location(
            Bucket=bucket,
        )
    except ClientError as e:
        logger.error(e)
        raise
    else:
        logger.info(f"Bucket {bucket}, exists, proceeding with deployment ...")
        return check_bucket           


def retrieve_existing_notification_configuration(bucket):
    try:
        existing_response = s3Client.get_bucket_notification_configuration(
            Bucket=bucket,
        )
    except ClientError as e:
        logger.error(e)
        raise
    else:                        
        existing_response.pop('ResponseMetadata')
        logger.info(f'Existing S3 Bucket {bucket} notification configuration is {existing_response}')
        return existing_response 


def put_notification_configuration(bucket):
    existing_config = retrieve_existing_notification_configuration(bucket)                      
    existing_config['EventBridgeConfiguration'] = {}
    try:
        response = s3Client.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration=existing_config,
            SkipDestinationValidation=True
        )
    except ClientError as e:
        logger.error(e)
        raise
    else:                     
        logger.info(f"S3 Response for put_notification_configuration is {response}")                                    


def lambda_handler(event, context):
    # Define Environmental Variables
    s3Bucket  = event.get('ResourceProperties').get('SourceBucket')

    logger.info(f'Event detail is: {event}')

    if event.get('RequestType') == 'Create' or event.get('RequestType') == 'Update':
      # logger.info(event)
      try:
          logger.info("Stack event is Create, checking if S3 bucket exists...")
          check_bucket_exists(s3Bucket)
          logger.info("Enabling Eventbridge for Amazon S3 Notification")
          put_notification_configuration(s3Bucket)
          responseData = {}
          responseData['message'] = "Successful"
          logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
          cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)
      except Exception as e:
          logger.error(e)
          responseData = {}
          responseData['message'] = str(e)
          failure_reason = str(e) 
          logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
          cfnresponse.send(event, context, cfnresponse.FAILED, responseData, reason=failure_reason)


    elif event.get('RequestType') == 'Delete':
        logger.info(event)
        try:
            logger.info(f"Stack event is Delete, nothing to do....")
            responseData = {}
            responseData['message'] = "Completed"
            logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
            cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)
        except Exception as e:
            logger.error(e)
            responseData = {}
            responseData['message'] = str(e)
            logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
            cfnresponse.send(event, context, cfnresponse.FAILED, responseData)                  
