import boto3
from botocore.exceptions import ClientError

s3_client = boto3.client(
    's3',
    endpoint_url='http://192.168.107.2:9000',  # Replace with your MinIO server address
    aws_access_key_id='MinioAdmin123',  # Replace with your access key
    aws_secret_access_key='MinioAdmin123',  # Replace with your secret key
    region_name='local'  # Replace with your region
)

# Function to create buckets in MinIO
def create_buckets():
    buckets_to_create = ["landing-zone", "bronze-zone", "silver-zone", "gold-zone"]
    
    for bucket in buckets_to_create:
        try:
            s3_client.create_bucket(Bucket=bucket, CreateBucketConfiguration={'LocationConstraint': 'local'})
            print(f"Bucket {bucket} created successfully.")
        except Exception as e:
            print(f"Error creating bucket {bucket}: {e}")

if __name__ == "__main__":
    create_buckets()
