# AWS-Boto3-Sync
AWS file sync using Boto3

# Why
There should be no need for this, but somehow for very large files, "aws s3 sync" doesn't quite cut it.
This is an attempt to work around that by leveraging AWS Boto3 library and multipart uploads.
If you think about it, it's not most efficient to download files from one S3 bucket and then upload to another.
The consolation prize though is that it will check and see if files already exist. Obviously, files could have changed.
The next todo is to check timestamps and checksums, etc.

# How
There are some outdated and confusing info about AWS Boho3 out there. From what I can gather, it seems there is no need
to roll your own multipart upload, while Boto3 library should take care of it automatically, if the appropriate S3 transfer
config is provided.

In order to run the script, appropriate source and destination profiles should be set up with "aws configure --profile"
(or just edit ~/.aws/* files manually). If it's not AWS, but rather an AWS S3 compatible service, you may need to specify the
endpoint and port. If there is error with SSL signing certificate, specify Verify=False for client('s3') in code.
Adjust parameters for TransferConfig to see what works the best. This will depend on the quality and speed of your network.

Separately, sync.py is a sample for syncing files between two local directories -- kind of like rsync,
sans mod-time/size/checksum verifications. It's included because.
There is also some sample data files in data/src for testing purpose.
