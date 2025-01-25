from faker import Faker
import boto3
import pandas as pd
from botocore.exceptions import ClientError
import random
import time


fake = Faker()
fake.seed_instance(0)

s3_client = boto3.client(
    's3',
    endpoint_url='http://192.168.107.2:9000',  # Substitua pelo endereço do seu servidor MinIO
    aws_access_key_id='MinioAdmin123',  # Substitua pela sua chave de acesso
    aws_secret_access_key='MinioAdmin123',  # Substitua pela sua chave secreta
    region_name='local'  # Substitua pela sua região
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
        # Tente criar o bucket se não existir
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'local'})
    except ClientError as e:
        if e.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
            print(f"Erro: {e}")
            return

    # Converter dados para um DataFrame
    df = pd.DataFrame(data)
    
    # Salvar dados em um arquivo Parquet localmente primeiro
    local_file_path = f'/tmp/{file_name}'  # Caminho local temporário
    df.to_parquet(local_file_path, index=False)

    # Fazer upload do arquivo Parquet local para o MinIO
    key = f"dataway/cloud_x/clients/{file_name}"
    s3_client.upload_file(local_file_path, bucket_name, key)
    print(f"Dados salvos em {bucket_name}/{key}")

    # Opcionalmente, remover o arquivo local após o upload
    import os
    os.remove(local_file_path)

if __name__ == "__main__":
    num_records = random.randint(1000, 10000)
    print(f"Número de registros a serem gerados: {num_records}")
    fake_data = generate_fake_data(num_records)
    
    file_name = f"clients_data_{int(time.time())}.parquet"  # Nome de arquivo dinâmico
    
    save_to_minio("landing-zone", file_name, fake_data)  # Substitua pelo nome do seu bucket
