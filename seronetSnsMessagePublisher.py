import boto3
import json

def sns_publisher(result,TopicArn_Success,TopicArn_Failure):
 try:
  sns = boto3.client('sns')
  # Publish a simple message to the specified SNS topic
  #if copy successfully
  resultJson=json.dumps(result)
  if(result['file_status']=="COPY_SUCCESSFUL" or result['file_status']=='FILE_Processed' or result['file_status']=="COPY_SUCCESSFUL_DUPLICATE"):
    response = sns.publish(TopicArn=TopicArn_Success, Message=str(resultJson), MessageAttributes={"TargetCBC": {"DataType": "String", "StringValue": result['file_submitted_by']}} )
  #if copy unsuccessfully
  elif(result['file_status']=="COPY_UNSUCCESSFUL" or result['file_status']=="COPY_UNSUCCESSFUL_DUPLICATE"):
    response = sns.publish(TopicArn=TopicArn_Failure, Message=str(resultJson), MessageAttributes={"TargetCBC": {"DataType": "String", "StringValue": result['file_submitted_by']}})
  # return out the response
  return response
 except Exception as err:
  raise err
