import boto3
import hashlib
import json
import datetime
import dateutil.tz



def get_stream_md5(stream):
  '''
    Get MD5 of a stream content as a hex encoded string
    :param stream:
    :return: hex encoded MD5
  '''
  BLOCK_SIZE=1024
  hash_obj = hashlib.md5()
  buf = stream.read(amt=BLOCK_SIZE)
  while len(buf) > 0:
      hash_obj.update(buf)
      buf = stream.read(amt=BLOCK_SIZE)
  
  return hash_obj.hexdigest()


def fileCopy(s3_client, event, destination_bucket_name):
  try:
      s3_client = boto3.client("s3")
      # defining constants for CBCs
      CBC01='cbc01'
      CBC02='cbc02'
      CBC03='cbc03'
      CBC04='cbc04'
      
      
      # Bucket Name where file was uploaded
      message = event['Records'][0]['Sns']['Message']
      #convert the message to json style
      messageJson = json.loads(message)
      source_bucket_name =messageJson['bucketName']
    
      # determining which cbc bucket the file came from
      prefix = '' 
    
      # Filename of object (with path) and Etag
      file_key_name = messageJson['key']
      
      obj = s3_client.get_object(Bucket=source_bucket_name, Key=file_key_name)
      stream_body = obj['Body']
      file_md5_source = get_stream_md5(stream_body)
      print('Source md5: '+file_md5_source)
    
      if CBC01 in source_bucket_name:
        prefix = CBC01
      elif  CBC02 in source_bucket_name:
        prefix = CBC02
      elif  CBC03 in source_bucket_name:
        prefix = CBC03
      elif  CBC04 in source_bucket_name:
        prefix = CBC04
      else:
        prefix='UNMATCHED'    
        
      # Setting the Timezone to US Eastern to prefix the object
      eastern = dateutil.tz.gettz('US/Eastern')
      timestamp = datetime.datetime.now(tz=eastern).strftime("%H-%M-%S-%m-%d-%Y")
      timestampDB = datetime.datetime.now(tz=eastern).strftime('%Y-%m-%d %H:%M:%S')
      
      # S3 copy object operation with the desired prefix
      key = prefix+'/'+timestamp+'/'+file_key_name
      copy_source_object = {'Bucket': source_bucket_name, 'Key': file_key_name}
      #set up the parameter for future RDS record
      file_name = file_key_name
      file_location = destination_bucket_name+'/'+key
      file_added_on = timestampDB
      file_last_processed_on = timestampDB
      file_origin = source_bucket_name
      fileType = file_key_name.split('.')
      if len(fileType)>1:
                file_type=fileType[len(fileType)-1]
      else:
                file_type="uncertain"
      file_action = "submit"
      file_submitted_by = prefix
      updated_by = None
            
      
      s3_client.copy_object(CopySource=copy_source_object, Bucket=destination_bucket_name, Key=key)
      #get destination file md5
      obj_dest = s3_client.get_object(Bucket=destination_bucket_name, Key=key)
      stream_body_dest = obj_dest['Body']
      file_md5_dest = get_stream_md5(stream_body_dest)

      
      print('Destination md5: '+file_md5_dest)
      if(file_md5_dest == file_md5_source):
                file_status= 'COPY_SUCCESSFUL'
                s3_client.delete_object(Bucket=source_bucket_name, Key=file_key_name)
      else:
                file_status= 'COPY_UNSUCCESSFUL'
                
      result={'file_name': file_name, 'file_location': file_location, 'file_added_on': file_added_on, 'file_last_processed_on': file_last_processed_on, 'file_status': file_status, 'file_origin': file_origin, 'file_type': file_type, 'file_action': file_action, 'file_submitted_by': file_submitted_by, 'updated_by': updated_by, 'file_md5':file_md5_source}
      return result
      
  except Exception as err:
      raise err






















