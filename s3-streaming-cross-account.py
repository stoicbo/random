import boto3
import botocore
from botocore.config import Config

def assume_role(role_arn, session_name="CrossAccountSession"):
    sts_client = boto3.client('sts')
    response = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name
    )
    credentials = response['Credentials']
    return boto3.client(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        config=Config(signature_version='s3v4')  # Necessary for secure S3 operations
    )

def copy_object(source_s3_client, destination_s3_client, source_bucket, source_key, destination_bucket, destination_key):
    # Initiate multi-part upload
    response = destination_s3_client.create_multipart_upload(
        Bucket=destination_bucket,
        Key=destination_key
    )
    upload_id = response['UploadId']
    part_number = 1
    part_info = {'Parts': []}

    try:
        # Stream and upload object in chunks
        response = source_s3_client.get_object(Bucket=source_bucket, Key=source_key)
        for chunk in response['Body'].iter_chunks(chunk_size=5 * 1024 * 1024):  # 5 MB chunks
            part_response = destination_s3_client.upload_part(
                Bucket=destination_bucket,
                Key=destination_key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=chunk
            )
            part_info['Parts'].append({
                'PartNumber': part_number,
                'ETag': part_response['ETag']
            })
            part_number += 1

        # Complete the multi-part upload
        destination_s3_client.complete_multipart_upload(
            Bucket=destination_bucket,
            Key=destination_key,
            UploadId=upload_id,
            MultipartUpload=part_info
        )
        print(f"Successfully copied {source_key} to {destination_key}")

    except Exception as e:
        # Abort the multi-part upload in case of failure
        destination_s3_client.abort_multipart_upload(
            Bucket=destination_bucket,
            Key=destination_key,
            UploadId=upload_id
        )
        print(f"Failed to copy {source_key}: {e}")
        raise

def copy_objects_with_prefix(source_bucket, prefix, destination_bucket, role_arn):
    # Assume role to access source account
    source_s3_client = assume_role(role_arn)
    destination_s3_client = boto3.client('s3')

    # List objects in the source bucket with the given prefix
    paginator = source_s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=source_bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                source_key = obj['Key']
                destination_key = source_key  # Maintain the same key structure in the destination bucket
                print(f"Copying {source_key} to {destination_bucket}/{destination_key}")
                copy_object(source_s3_client, destination_s3_client, source_bucket, source_key, destination_bucket, destination_key)

# Example usage
source_bucket = "source-bucket-name"
prefix = "path/to/prefix/"  # The prefix to copy objects from
destination_bucket = "destination-bucket-name"
role_arn = "arn:aws:iam::source-account-id:role/SourceAccountRole"

copy_objects_with_prefix(source_bucket, prefix, destination_bucket, role_arn)
