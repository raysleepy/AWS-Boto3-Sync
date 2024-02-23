import os
import boto3
from boto3.s3.transfer import TransferConfig

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
# Per doc above, max 10k multiparts, so do your math accordingly. 100MB should be a good chunksize, i.e., will support up to 1TB
MB = 1024 ** 2
GB = 1024 ** 3
s3_transfer_config = TransferConfig(
        multipart_threshold = 1*MB,
        multipart_chunksize = 1*MB,
        use_threads = False,
        max_concurrency = 1
    )

src_bucket = 'ray-boto-source'
dst_bucket = 'ray-boto-dest'

s3_src = boto3.Session(profile_name='boto-source').client('s3')
s3_dst = boto3.Session(profile_name='boto-dest').client('s3')

dst_keys = []
dst = s3_dst.list_objects(Bucket=dst_bucket)
if any('Contents' in x for x in dst):
    dst_keys = [x['Key'] for x in dst['Contents']]

for item in s3_src.list_objects(Bucket=src_bucket)['Contents']:
    key: str = item['Key']
    filename = key.split('/')[-1]
    local_path = os.path.join('data', 'tmp', filename)
    if key in dst_keys:
        print(f"Skipping {filename}")
    else:
        try:
            s3_src.download_file(src_bucket, key, local_path)
            print(f"Uploading {filename} to s3://{dst_bucket}/{key}")
            s3_dst.upload_file(local_path, dst_bucket, key, Config=s3_transfer_config)
            os.remove(local_path)
        except Exception as e:
            print(f"Error: {e}")
