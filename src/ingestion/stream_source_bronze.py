import os
import json
import time
from datetime import datetime, timezone, timedelta

import awswrangler as wr
import boto3
import pandas as pd
import pymysql
from dotenv import load_dotenv, find_dotenv
from kafka import KafkaConsumer

# Load .env file if present (useful for local development outside Docker)
load_dotenv(find_dotenv())

# Get Kafka connection settings
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
topic_name = "tech-challenge.events"

# Get database connection settings
db_host = os.getenv("DB_HOST", "localhost")
db_port = int(os.getenv("DB_PORT", 3306))
db_user = os.getenv("DB_USER", "root")
db_password = os.getenv("DB_PASSWORD", "password")
db_database = os.getenv("DB_DATABASE", "tech")

# Get AWS / S3 settings
aws_region = os.getenv("AWS_REGION", "us-east-1")
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID", "")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
aws_bucket = os.getenv("AWS_BUCKET", "")
project_environment = os.getenv("PROJECT_ENVIRONMENT", "dev")
bronze_base_path = os.getenv("BRONZE_BASE_PATH", "bronze")
bronze_target_name = os.getenv("BRONZE_TARGET_NAME", "uf")

# S3 path follows the same convention as the batch pipeline:
# s3://{bucket}/{environment}/{bronze_base_path}/{target_name}/
s3_bronze_path = (
    f"s3://{aws_bucket}/{project_environment}/{bronze_base_path}/{bronze_target_name}/"
)

# Configure boto3 session for awswrangler — explicit credentials ensure the
# session works both locally (via .env) and inside Docker containers.
boto3.setup_default_session(
    region_name=aws_region,
    aws_access_key_id=aws_access_key_id or None,
    aws_secret_access_key=aws_secret_access_key or None,
)

def deserialize_value(value):
    if value is None:
        return None
    try:
        return json.loads(value.decode("utf-8"))
    except Exception:
        return None

def validate_event(event_data):
    if not isinstance(event_data, dict):
        return False
    required_fields = ["ano", "sigla_uf", "serie", "rede", "taxa_alfabetizacao", "media_portugues"]
    return all(event_data.get(field) is not None for field in required_fields)


def append_event_to_s3_bronze(event_data: dict, kafka_offset: int) -> None:
    """
    Appends a single streaming event to the Bronze parquet dataset in S3.

    Mirrors the schema and metadata conventions of the batch pipeline
    (ingest_bronze_batch.py), using mode='append' so existing partitions
    are preserved. The row is partitioned by _ingestion_date.

    Parameters
    ----------
    event_data : dict
        Raw event payload from Kafka (already validated).
    kafka_offset : int
        Kafka message offset, used as part of the execution_id for traceability.
    """
    if not aws_bucket:
        print("AWS_BUCKET not configured — skipping S3 write.")
        return

    brasilia_tz = timezone(timedelta(hours=-3))
    ingestion_timestamp = datetime.now(brasilia_tz)
    execution_id = f"stream_{ingestion_timestamp.strftime('%Y%m%dT%H%M%SZ')}_offset{kafka_offset}"

    row = {
        # Business columns
        "ano": event_data.get("ano"),
        "sigla_uf": event_data.get("sigla_uf"),
        "serie": event_data.get("serie"),
        "rede": event_data.get("rede"),
        "taxa_alfabetizacao": event_data.get("taxa_alfabetizacao"),
        "media_portugues": event_data.get("media_portugues"),
        "proporcao_aluno_nivel_0": event_data.get("proporcao_aluno_nivel_0"),
        "proporcao_aluno_nivel_1": event_data.get("proporcao_aluno_nivel_1"),
        "proporcao_aluno_nivel_2": event_data.get("proporcao_aluno_nivel_2"),
        "proporcao_aluno_nivel_3": event_data.get("proporcao_aluno_nivel_3"),
        "proporcao_aluno_nivel_4": event_data.get("proporcao_aluno_nivel_4"),
        "proporcao_aluno_nivel_5": event_data.get("proporcao_aluno_nivel_5"),
        "proporcao_aluno_nivel_6": event_data.get("proporcao_aluno_nivel_6"),
        "proporcao_aluno_nivel_7": event_data.get("proporcao_aluno_nivel_7"),
        "proporcao_aluno_nivel_8": event_data.get("proporcao_aluno_nivel_8"),
        # Bronze metadata — mirrors add_bronze_metadata() from the batch pipeline
        "_source_system": "kafka_stream",
        "_source_name": bronze_target_name,
        "_source_dataset_id": "br_inep_avaliacao_alfabetizacao",
        "_source_table_id": "uf",
        "_ingestion_timestamp_utc": ingestion_timestamp.isoformat(),
        "_ingestion_date": ingestion_timestamp.date().isoformat(),
        "_execution_id": execution_id,
    }

    df = pd.DataFrame([row])

    wr.s3.to_parquet(
        df=df,
        path=s3_bronze_path,
        dataset=True,
        mode="append",
        partition_cols=["_ingestion_date"],
        compression="snappy",
    )

    print(f"Appended event to S3 Bronze: {s3_bronze_path} | execution_id={execution_id}")

def main():
    # Connect to Kafka with retry logic
    consumer = None
    for attempt in range(10):
        try:
            print(f"Connecting to Kafka at {bootstrap_servers} (attempt {attempt+1}/10)...")
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=bootstrap_servers.split(","),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=deserialize_value
            )
            print("Connected to Kafka successfully.")
            break
        except Exception as e:
            print(f"Kafka connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

    if not consumer:
        print("Failed to connect to Kafka after 10 attempts. Exiting.")
        exit(1)

    # Connect to Database with retry logic
    db_connection = None
    for attempt in range(10):
        try:
            print(f"Connecting to database '{db_database}' on '{db_host}:{db_port}' as '{db_user}' (attempt {attempt+1}/10)...")
            db_connection = pymysql.connect(
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_password,
                database=db_database
            )
            print("Connected to database successfully.")
            break
        except Exception as e:
            print(f"Database connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

    if not db_connection:
        print("Failed to connect to database after 10 attempts. Exiting.")
        consumer.close()
        exit(1)

    print(f"Listening for events on topic '{topic_name}'...")

    insert_query = """
    INSERT INTO br_inep_avaliacao_alfabetizacao_uf 
    (ano, sigla_uf, serie, rede, taxa_alfabetizacao, media_portugues, 
     proporcao_aluno_nivel_0, proporcao_aluno_nivel_1, proporcao_aluno_nivel_2, 
     proporcao_aluno_nivel_3, proporcao_aluno_nivel_4, proporcao_aluno_nivel_5, 
     proporcao_aluno_nivel_6, proporcao_aluno_nivel_7, proporcao_aluno_nivel_8) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        for message in consumer:
            event_data = message.value
            
            if not validate_event(event_data):
                print(f"Skipping invalid event format at offset {message.offset}: {event_data}")
                continue
                
            try:
                ano = int(event_data.get("ano"))
                sigla_uf = str(event_data.get("sigla_uf"))
                serie = int(event_data.get("serie"))
                rede = int(event_data.get("rede"))
                taxa_alfabetizacao = float(event_data.get("taxa_alfabetizacao"))
                media_portugues = float(event_data.get("media_portugues"))
                
                p0 = int(event_data.get("proporcao_aluno_nivel_0")) if event_data.get("proporcao_aluno_nivel_0") is not None else None
                p1 = float(event_data.get("proporcao_aluno_nivel_1")) if event_data.get("proporcao_aluno_nivel_1") is not None else None
                p2 = float(event_data.get("proporcao_aluno_nivel_2")) if event_data.get("proporcao_aluno_nivel_2") is not None else None
                p3 = float(event_data.get("proporcao_aluno_nivel_3")) if event_data.get("proporcao_aluno_nivel_3") is not None else None
                p4 = float(event_data.get("proporcao_aluno_nivel_4")) if event_data.get("proporcao_aluno_nivel_4") is not None else None
                p5 = float(event_data.get("proporcao_aluno_nivel_5")) if event_data.get("proporcao_aluno_nivel_5") is not None else None
                p6 = float(event_data.get("proporcao_aluno_nivel_6")) if event_data.get("proporcao_aluno_nivel_6") is not None else None
                p7 = float(event_data.get("proporcao_aluno_nivel_7")) if event_data.get("proporcao_aluno_nivel_7") is not None else None
                p8 = float(event_data.get("proporcao_aluno_nivel_8")) if event_data.get("proporcao_aluno_nivel_8") is not None else None
                
                with db_connection.cursor() as cursor:
                    cursor.execute(insert_query, (ano, sigla_uf, serie, rede, taxa_alfabetizacao, media_portugues, p0, p1, p2, p3, p4, p5, p6, p7, p8))
                db_connection.commit()
                
                print(f"Successfully inserted event (Offset: {message.offset}) -> Year: {ano}, UF: {sigla_uf}, Rate: {taxa_alfabetizacao}")

                # Append the same row to the Bronze parquet dataset in S3
                try:
                    append_event_to_s3_bronze(event_data, kafka_offset=message.offset)
                except Exception as s3_err:
                    # S3 write failure must not roll back the DB insert
                    print(f"Warning: S3 write failed for offset {message.offset}: {s3_err}")

            except Exception as insert_err:
                print(f"Failed to process message at offset {message.offset}: {insert_err}")
                db_connection.rollback()
                
    except KeyboardInterrupt:
        print("\nStopping consumer stream...")
    finally:
        consumer.close()
        db_connection.close()
        print("Connections closed. Consumer stopped successfully.")

if __name__ == "__main__":
    main()
