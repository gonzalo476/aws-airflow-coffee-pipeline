def upload_to_s3(local_file: str, bucket: str, key: str) -> None:
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client("s3")
    
    try:
        s3.upload_file(local_file, bucket, key)
        print(f"File successfully uploaded to {bucket}")
    except FileNotFoundError:
        print("File does not exists")
    except ClientError as e:
        print(f"Error while uploading the file: {e}")