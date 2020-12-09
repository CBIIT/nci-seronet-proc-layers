import urllib
import zipfile
import boto3
from io import *
import json
import os
import datetime
import dateutil.tz
import logging


def fileCopy(s3_client, event, destination_bucket_name):
  try:
      # defining constants for CBCs
      CBC01='cbc01'
      CBC02='cbc02'
      CBC03='cbc03'
      CBC04='cbc04'
      
      
      # Bucket Name where file was uploaded
      #
      message=event['Records'][0]['Sns']['Message']
      #convert the message to json style
      messageJson=json.loads(message)
      source_bucket_name =messageJson['bucketName']
    
      # determining which cbc bucket the file came from
      prefix='' 
    
      # Filename of object (with path) and Etag
      file_key_name = messageJson['key']
      
      source_etag = s3_client.head_object(Bucket=source_bucket_name,Key=file_key_name)['ETag'][1:-1]
      
    
      if CBC01 in source_bucket_name:
        prefix=CBC01
      elif  CBC02 in source_bucket_name:
        prefix=CBC02
      elif  CBC03 in source_bucket_name:
        prefix=CBC03
      elif  CBC04 in source_bucket_name:
        prefix=CBC04        
      else:
        prefix='UNMATCHED'    
        
      # Setting the Timezone to US Eastern to prefix the object    
      eastern = dateutil.tz.gettz('US/Eastern')
      timestamp=datetime.datetime.now(tz=eastern).strftime("%H-%M-%S-%m-%d-%Y")
      timestampDB=datetime.datetime.now(tz=eastern).strftime('%Y-%m-%d %H:%M:%S')
      
      # S3 copy object operation with the desired prefix
      key =prefix+'/'+timestamp+'/'+file_key_name
      copy_source_object = {'Bucket': source_bucket_name, 'Key': file_key_name}
      #set up the parameter for future RDS record
      file_name="'"+file_key_name+"'"
      file_location="'"+destination_bucket_name+'/'+key+"'"
      file_added_on="'"+timestampDB+"'"
      file_last_processed_on="'"+timestampDB+"'"
      file_origin="'"+source_bucket_name+"'"
      fileType=file_key_name.split('.')
      if len(fileType)>1:
                file_type="'"+fileType[len(fileType)-1]+"'"
      else:
                file_type="'"+"uncertain"+"'"
      file_action="'"+"submit"+"'" 
      file_submitted_by= "'"+prefix+ "'"
      updated_by="NULL"
            
      
      s3_client.copy_object(CopySource=copy_source_object, Bucket=destination_bucket_name, Key=key)
      dest_etag = s3_client.head_object(Bucket=destination_bucket_name,Key=key)['ETag'][1:-1]
      print('Destination Etag: '+dest_etag)
      if(dest_etag==source_etag):
                file_status= "'"+'COPY_SUCCESSFUL'+"'"
                s3_client.delete_object(Bucket=source_bucket_name, Key=file_key_name)
      else:
                file_status= "'"+'COPY_UNSUCCESSFUL'+"'"
                
      result={'file_name': file_name, 'file_location': file_location, 'file_added_on': file_added_on, 'file_last_processed_on': file_last_processed_on, 'file_status': file_status, 'file_origin': file_origin, 'file_type': file_type, 'file_action': file_action, 'file_submitted_by': file_submitted_by, 'updated_by': updated_by}
      return result
      
  except Exception as err:
      raise err
