import os
import json
import time
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv, find_dotenv

# Load .env first — must happen before boto3 so env vars are available.
load_dotenv(find_dotenv())

# AWS_VERIFY_SSL=false disables SSL cert verification (useful on Windows dev
# environments where the system CA is not in certifi's bundle).
# Botocore builds its own urllib3 PoolManager with an explicit ssl_context,
# so we must patch BotocoreHTTPSession directly — ssl._create_default_https_context
# is NOT respected by botocore.
_verify_ssl = os.getenv("AWS_VERIFY_SSL", "true").lower() not in ("false", "0", "no")

import botocore.httpsession  # noqa: E402

if not _verify_ssl:
    import urllib3  # noqa: E402
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    _orig_botocore_http_init = botocore.httpsession.URLLib3Session.__init__

    def _botocore_no_ssl_verify(self, *args, **kwargs):
        kwargs["verify"] = False
        _orig_botocore_http_init(self, *args, **kwargs)

    botocore.httpsession.URLLib3Session.__init__ = _botocore_no_ssl_verify

import awswrangler as wr  # noqa: E402
import boto3              # noqa: E402
import pandas as pd       # noqa: E402
import pymysql            # noqa: E402
from kafka import KafkaConsumer  # noqa: E402

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
# Glue Data Catalog settings (optional — mirrors --register-catalog in the batch pipeline)
bronze_database_name = os.getenv("BRONZE_DATABASE_NAME", "bronze")
register_catalog = os.getenv("REGISTER_CATALOG", "false").lower() in ("true", "1", "yes")

# S3 path follows the same convention as the batch pipeline:
# s3://{bucket}/{environment}/{bronze_base_path}/{target_name}/
s3_bronze_path = (
    f"s3://{aws_bucket}/{project_environment}/{bronze_base_path}/{bronze_target_name}/"
)

# Configure boto3 session for awswrangler — explicit credentials ensure the
# session works both locally (via .env) and inside Docker containers.
# aws_session_token is required when using temporary STS credentials (ASIA* keys).
boto3.setup_default_session(
    region_name=aws_region,
    aws_access_key_id=aws_access_key_id or None,
    aws_secret_access_key=aws_secret_access_key or None,
    aws_session_token=os.getenv("AWS_SESSION_TOKEN") or None,
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


# Natural key used to deduplicate when merging new events with existing data.
_NATURAL_KEY = ["ano", "sigla_uf", "serie", "rede"]


def read_existing_bronze(path: str) -> pd.DataFrame | None:
    """
    Lê o dataset Bronze existente no S3.

    Returns None if the path does not exist yet (first run).
    """
    try:
        existing = wr.s3.read_parquet(path=path, dataset=True)
        print(f"Read {len(existing)} existing rows from {path}")
        return existing
    except Exception:
        # Path doesn't exist yet or is empty — fresh start
        return None


def merge_event_into_dataset(
    existing_df: pd.DataFrame | None,
    new_row: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merges the new Kafka row into the existing dataset.

    If a row with the same natural key (ano + sigla_uf + serie + rede) already
    exists, the incoming event overwrites it. New rows are simply appended.
    """
    if existing_df is None or existing_df.empty:
        return new_row

    # Build a boolean mask of rows that share the same natural key as the new row
    mask = pd.Series([True] * len(existing_df), index=existing_df.index)
    for col in _NATURAL_KEY:
        if col in new_row.columns and col in existing_df.columns:
            mask = mask & (existing_df[col] == new_row[col].iloc[0])

    deduplicated = existing_df[~mask]
    merged = pd.concat([deduplicated, new_row], ignore_index=True)
    print(
        f"Merged: {len(existing_df)} existing + 1 new → {len(merged)} rows "
        f"(replaced {int(mask.sum())} duplicate(s))"
    )
    return merged


def write_bronze_to_s3(df: pd.DataFrame, path: str) -> None:
    """
    Saves the full merged DataFrame back to S3 as a partitioned Parquet dataset.

    Mirrors write_bronze_parquet_to_s3() from the batch pipeline:
      - mode='overwrite'   — full replace, consistent with batch behaviour
      - partition_cols     — partitioned by _ingestion_date
      - register_catalog   — optionally registers/updates the Glue table
    """
    if register_catalog:
        print(
            f"Writing Parquet + registering Glue table: "
            f"database={bronze_database_name} | table={bronze_target_name}"
        )
        wr.s3.to_parquet(
            df=df,
            path=path,
            dataset=True,
            mode="overwrite",
            partition_cols=["_ingestion_date"],
            compression="snappy",
            database=bronze_database_name,
            table=bronze_target_name,
        )
    else:
        wr.s3.to_parquet(
            df=df,
            path=path,
            dataset=True,
            mode="overwrite",
            partition_cols=["_ingestion_date"],
            compression="snappy",
        )


def append_event_to_s3_bronze(event_data: dict, kafka_offset: int) -> None:
    """
    Merges a single streaming event into the Bronze Parquet dataset in S3.

    Workflow (mirrors the batch pipeline convention):
      1. Read the existing dataset from S3 (if any).
      2. Build the new row with Bronze metadata.
      3. Merge: deduplicate on natural key (ano+sigla_uf+serie+rede), then concat.
      4. Overwrite the full dataset back to S3.
      5. Optionally register/update the Glue Data Catalog table.

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

    new_row = pd.DataFrame([{
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
    }])

    # 1. Read existing dataset
    existing_df = read_existing_bronze(s3_bronze_path)

    # 2. Merge new row (deduplicates on natural key)
    merged_df = merge_event_into_dataset(existing_df, new_row)

    # 3. Overwrite full dataset (+ optional Glue registration)
    write_bronze_to_s3(merged_df, s3_bronze_path)

    print(
        f"S3 Bronze updated: {s3_bronze_path} | "
        f"total_rows={len(merged_df)} | execution_id={execution_id}"
    )

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
