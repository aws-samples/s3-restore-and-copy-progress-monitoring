
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
    responseData1={}
    responseData2={}
    try:
        if event['RequestType'] == 'Delete':
            print("Request Type:",event['RequestType'])
            Bucket=event['ResourceProperties']['Bucket']
            TargetBucket=event['ResourceProperties']['TargetBucket']
            delete_notification1(Bucket)
            delete_notification2(TargetBucket)
            print("Sending response to custom resource after Delete")
        elif event['RequestType'] == 'Create' or event['RequestType'] == 'Update':
            print("Request Type:",event['RequestType'])
            LambdaArn1=event['ResourceProperties']['LambdaArn1']
            LambdaArn2=event['ResourceProperties']['LambdaArn2']
            LambdaArn3=event['ResourceProperties']['LambdaArn3']
            LambdaArn4=event['ResourceProperties']['LambdaArn4']
            Bucket=event['ResourceProperties']['Bucket']
            TargetBucket=event['ResourceProperties']['TargetBucket']
            # ManifestBucket=event['ResourceProperties']['ManifestBucket']
            add_notification1(LambdaArn1, LambdaArn2, LambdaArn3, Bucket)
            add_notification2(LambdaArn4, TargetBucket)
            responseData1={'Bucket':Bucket}
            responseData2={'Bucket':TargetBucket}
            print("Sending response to custom resource")
        responseStatus = 'SUCCESS'
    except Exception as e:
        print('Failed to process:', e)
        responseStatus = 'FAILED'
        responseData1 = {'Failure': 'Something bad happened.'}
        responseData2 = {'Failure': 'Something bad happened.'}
    cfnresponse.send(event, context, responseStatus, responseData1, "CustomResourcePhysicalID")
    cfnresponse.send(event, context, responseStatus, responseData2, "CustomResourcePhysicalID")

def add_notification1(LambdaArn1, LambdaArn2, LambdaArn3,Bucket):
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
        ]
      }
    )
    print("Put request completed....")

def add_notification2(LambdaArn4,TargetBucket):
    bucket_notification = s3.BucketNotification(TargetBucket)
    response = bucket_notification.put(
      NotificationConfiguration={
        'LambdaFunctionConfigurations': [
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
  
def delete_notification1(Bucket):
    bucket_notification = s3.BucketNotification(Bucket)
    response = bucket_notification.put(
        NotificationConfiguration={}
    )
    print("Delete request completed....")

def delete_notification2(TargetBucket):
    bucket_notification = s3.BucketNotification(TargetBucket)
    response = bucket_notification.put(
        NotificationConfiguration={}
    )
    print("Delete request completed....")            

