
from __future__ import print_function
import json
import boto3
import cfnresponse

SUCCESS = "SUCCESS"
FAILED = "FAILED"

print('Loading function')
s3 = boto3.resource('s3')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    responseData={}
    try:
        if event['RequestType'] == 'Delete':
            print("Request Type:",event['RequestType'])
            Bucket=event['ResourceProperties']['Bucket']
            delete_notification(Bucket)
            print("Sending response to custom resource after Delete")
        elif event['RequestType'] == 'Create' or event['RequestType'] == 'Update':
            print("Request Type:",event['RequestType'])
            LambdaArn1=event['ResourceProperties']['LambdaArn1']
            LambdaArn2=event['ResourceProperties']['LambdaArn2']
            LambdaArn3=event['ResourceProperties']['LambdaArn3']
            LambdaArn4=event['ResourceProperties']['LambdaArn4']
            Bucket=event['ResourceProperties']['Bucket']
            # ManifestBucket=event['ResourceProperties']['ManifestBucket']
            add_notification(LambdaArn1, LambdaArn2, LambdaArn3, LambdaArn4, Bucket)
            # add_notification1(LambdaArn1, ManifestBucket)
            responseData={'Bucket':Bucket}
            print("Sending response to custom resource")
        responseStatus = 'SUCCESS'
    except Exception as e:
        print('Failed to process:', e)
        responseStatus = 'FAILED'
        responseData = {'Failure': 'Something bad happened.'}
    cfnresponse.send(event, context, responseStatus, responseData, "CustomResourcePhysicalID")

def add_notification(LambdaArn1, LambdaArn2, LambdaArn3, LambdaArn4,Bucket):
    bucket_notification = s3.BucketNotification(Bucket)
    response = bucket_notification.put(
      NotificationConfiguration={
        'LambdaFunctionConfigurations': [
          {
              'LambdaFunctionArn': LambdaArn1,
              'Events': [
                  's3:ObjectRestore:Delete'
              ]
          },
          {
              'LambdaFunctionArn': LambdaArn2,
              'Events': [
                  's3:ObjectRestore:Post'
              ]
          },
          {
              'LambdaFunctionArn': LambdaArn3,
              'Events': [
                  's3:ObjectRestore:Completed'
              ]
          },
          {
              'LambdaFunctionArn': LambdaArn4,
              'Events': [
                  's3:ObjectCreated:Copy',
                  's3:ObjectCreated:CompleteMultipartUpload'
              ]
          },
        ]
      }
    )
    print("Put request completed....")  
  
def delete_notification(Bucket):
    bucket_notification = s3.BucketNotification(Bucket)
    response = bucket_notification.put(
        NotificationConfiguration={}
    )
    print("Delete request completed....")

