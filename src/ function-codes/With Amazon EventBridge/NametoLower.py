import cfnresponse


def lambda_handler(event, context):
    to_lower = event['ResourceProperties'].get('stackname', '').lower()
    responseData = dict(change_to_lower=to_lower)
    cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)
