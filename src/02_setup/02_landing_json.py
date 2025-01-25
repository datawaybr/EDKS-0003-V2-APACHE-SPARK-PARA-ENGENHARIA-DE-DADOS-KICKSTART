from faker import Faker
import boto3
import json
from botocore.exceptions import ClientError
import random
import time

fake = Faker()
fake.seed_instance(0)

s3_client = boto3.client(
    's3',
    endpoint_url='http://192.168.107.2:9000',  # Replace with your MinIO server address
    aws_access_key_id='MinioAdmin123',  # Replace with your access key
    aws_secret_access_key='MinioAdmin123',  # Replace with your secret key
    region_name='local'  # Replace with your region
)

# Função para gerar dados falsos
def generate_fake_data(num_records):
    data = []
    for _ in range(num_records):
        record = {
            "name": fake.name(),
            "address": fake.address(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
        }
        data.append(record)
    return data

# Função para salvar dados no MinIO usando Boto3
def save_to_minio(bucket_name, file_name, data):
    try:
        # Criar bucket se não existir
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'local'})
    except ClientError as e:
        if e.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
            print(f"Erro: {e}")
            return
    key = f"dataway/sap/clients/{file_name}"
    # Salvar dados em um arquivo JSON
    s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(data).encode('utf-8')
    )
    print(f"Dados salvos em {bucket_name}/{key}")

if __name__ == "__main__":
    num_records = random.randint(1000, 10000)
    print(f"Número de registros a serem gerados: {num_records}")
    fake_data = generate_fake_data(num_records)
    
    file_name = f"clients_data_{int(time.time())}.json"  # Nome de arquivo dinâmico
    
    save_to_minio("landing-zone", file_name, fake_data)  # Substitua pelo nome do seu bucket
