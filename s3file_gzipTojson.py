import boto3
import gzip
import io

print('Loading function')


def lambda_handler(event, context):
    # Get info from event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    bucket_dst = bucket + '.json'
    key_dst = key.replace('.gz', '')

    try:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket, Key=key)
        byteObj = gzip.GzipFile(None, 'rb', fileobj=io.BytesIO(obj['Body'].read()))

        print('###################')
        print('Event: ', event)
        print('Bucket: ', bucket)
        print('Key: ', key)
        print('###################')

        s3.upload_fileobj(Fileobj=byteObj, Bucket=bucket_dst, Key=key_dst)

    except Exception as e:
        print(e)
        print(
            'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(
                key, bucket))
        raise e