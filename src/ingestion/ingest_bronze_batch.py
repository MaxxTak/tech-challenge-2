import argparse
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from dotenv import load_dotenv, find_dotenv

# Permite importar src/utils/config.py mesmo rodando o script pela raiz do projeto
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

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

from src.utils.config import load_yaml  # noqa: E402

# Configure boto3 session for awswrangler — explicit credentials ensure the
# session works both locally (via .env) and inside Docker containers.
# aws_session_token is required when using temporary STS credentials (ASIA* keys).
boto3.setup_default_session(
    region_name=os.getenv("AWS_REGION", "us-east-1"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID") or None,
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY") or None,
    aws_session_token=os.getenv("AWS_SESSION_TOKEN") or None,
)



# Logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

# Funções auxiliares

def read_source_from_csv(
    csv_path: str,
    limit: int | None = None,
) -> pd.DataFrame:
    """
    Lê uma tabela a partir de um arquivo CSV local.

    Parameters
    ----------
    csv_path : str
        Caminho para o arquivo CSV (absoluto ou relativo à raiz do projeto).

    limit : int | None
        Limite de linhas para desenvolvimento. Use None para carga completa.

    Returns
    -------
    pd.DataFrame
        Dados lidos do CSV.
    """

    resolved = Path(csv_path) if Path(csv_path).is_absolute() else PROJECT_ROOT / csv_path

    logger.info("Lendo fonte CSV: %s | limit=%s", resolved, limit)

    df = pd.read_csv(resolved, nrows=limit)

    logger.info(
        "Fonte CSV lida com sucesso: rows=%s | columns=%s",
        len(df),
        len(df.columns),
    )

    return df


def read_source_from_basedosdados(
    dataset_id: str,
    table_id: str,
    billing_project_id: str,
    limit: int | None = None,
) -> pd.DataFrame:
    """
    Lê uma tabela da Base dos Dados via BigQuery.

    Parameters
    ----------
    dataset_id : str
        Nome do dataset na Base dos Dados, sem o prefixo 'basedosdados.'.
        Exemplo: 'br_inep_avaliacao_alfabetizacao'.

    table_id : str
        Nome da tabela dentro do dataset.
        Exemplo: 'uf'.

    billing_project_id : str
        Project ID do Google Cloud usado para autenticar/contabilizar a query.

    limit : int | None
        Limite de linhas para desenvolvimento. Use None para carga completa.

    Returns
    -------
    pd.DataFrame
        Dados lidos da fonte.
    """
    try:
        import basedosdados as bd
    except ImportError:
        raise ImportError(
            "O pacote 'basedosdados' não está instalado. "
            "Instale-o ou configure 'csv_path' na source para usar CSV local."
        )

    logger.info(
        "Lendo fonte Base dos Dados: dataset_id=%s | table_id=%s | limit=%s",
        dataset_id,
        table_id,
        limit,
    )

    df = bd.read_table(
        dataset_id=dataset_id,
        table_id=table_id,
        billing_project_id=billing_project_id,
        limit=limit,
    )

    logger.info(
        "Fonte lida com sucesso: dataset_id=%s | table_id=%s | rows=%s | columns=%s",
        dataset_id,
        table_id,
        len(df),
        len(df.columns),
    )

    return df


def add_bronze_metadata(
    df: pd.DataFrame,
    source_name: str,
    dataset_id: str,
    table_id: str,
    execution_id: str,
) -> pd.DataFrame:
    """
    Adiciona metadados técnicos de ingestão na camada Bronze.

    A Bronze deve preservar os dados de origem sem transformação de negócio.
    Esses campos são metadados técnicos para rastreabilidade.

    Parameters
    ----------
    df : pd.DataFrame
        Dados originais lidos da fonte.

    source_name : str
        Nome lógico da fonte no arquivo sources.yaml.

    dataset_id : str
        Dataset de origem da Base dos Dados.

    table_id : str
        Tabela de origem da Base dos Dados.

    execution_id : str
        Identificador único da execução do pipeline.

    Returns
    -------
    pd.DataFrame
        DataFrame com metadados técnicos adicionados.
    """

    brasilia_tz = timezone(timedelta(hours=-3))
    ingestion_timestamp = datetime.now(brasilia_tz)

    df = df.copy()

    df["_source_system"] = "base_dos_dados"
    df["_source_name"] = source_name
    df["_source_dataset_id"] = dataset_id
    df["_source_table_id"] = table_id
    df["_ingestion_timestamp_utc"] = ingestion_timestamp.isoformat()
    df["_ingestion_date"] = ingestion_timestamp.date().isoformat()
    df["_execution_id"] = execution_id

    return df


def build_s3_bronze_path(
    bucket: str,
    environment: str,
    bronze_base_path: str,
    target_name: str,
) -> str:
    """
    Monta o caminho S3 da camada Bronze.

    Exemplo:
    s3://bucket/dev/bronze/uf/
    """

    return f"s3://{bucket}/{environment}/{bronze_base_path}/{target_name}/"


def write_bronze_parquet_to_s3(
    df: pd.DataFrame,
    s3_path: str,
    database_name: str | None,
    table_name: str | None,
    register_catalog: bool,
) -> None:
    """
    Salva o DataFrame em Parquet no S3.

    Se register_catalog=True, também registra/atualiza a tabela no Glue Data Catalog,
    permitindo consulta direta pelo Athena.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a ser salvo.

    s3_path : str
        Caminho de destino no S3.

    database_name : str | None
        Nome do database no Glue/Athena. Exemplo: 'bronze'.

    table_name : str | None
        Nome da tabela no Glue/Athena. Exemplo: 'uf_raw'.

    register_catalog : bool
        Se True, registra no Glue Catalog.
    """

    logger.info("Salvando Parquet no S3: %s", s3_path)

    if register_catalog:
        if not database_name or not table_name:
            raise ValueError(
                "database_name e table_name são obrigatórios quando register_catalog=True."
            )

        logger.info(
            "Registrando tabela no Glue Catalog: database=%s | table=%s",
            database_name,
            table_name,
        )

        wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
            mode="overwrite",
            database=database_name,
            table=table_name,
            partition_cols=["_ingestion_date"],
            compression="snappy",
        )

    else:
        wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
            mode="overwrite",
            partition_cols=["_ingestion_date"],
            compression="snappy",
        )

    logger.info("Escrita concluída com sucesso: %s", s3_path)


def ingest_single_source(
    source_name: str,
    source_config: dict[str, Any],
    settings: dict[str, Any],
    execution_id: str,
    limit: int | None,
    register_catalog: bool,
) -> None:
    """
    Executa a ingestão Bronze de uma única fonte configurada no sources.yaml.

    Se a source definir 'csv_path', lê de um arquivo CSV local.
    Caso contrário, lê da Base dos Dados via BigQuery.
    """

    dataset_id = source_config["dataset_id"]
    table_id = source_config["table_id"]
    target_name = source_config["target_name"]
    csv_path = source_config.get("csv_path")

    bucket = settings["aws"]["bucket"]
    environment = settings["project"]["environment"]
    bronze_base_path = settings["bronze"]["base_path"]
    bronze_database_name = settings["bronze"]["database_name"]

    if environment == "dev" and limit is None:
        limit = 1000
        logger.info(f"Ambiente de {environment} e limit = None. Limite da ingestão alterado para {limit}.")

    s3_path = build_s3_bronze_path(
        bucket=bucket,
        environment=environment,
        bronze_base_path=bronze_base_path,
        target_name=target_name,
    )

    logger.info("=" * 80)
    logger.info("Iniciando ingestão da fonte: %s", source_name)
    logger.info("Destino Bronze: %s", s3_path)

    if csv_path:
        # Lê de arquivo CSV local — não requer credenciais Google/BigQuery
        df = read_source_from_csv(csv_path=csv_path, limit=limit)
    else:
        # Lê da Base dos Dados via BigQuery
        billing_project_id = settings["google"]["billing_project_id"]
        df = read_source_from_basedosdados(
            dataset_id=dataset_id,
            table_id=table_id,
            billing_project_id=billing_project_id,
            limit=limit,
        )

    df = add_bronze_metadata(
        df=df,
        source_name=source_name,
        dataset_id=dataset_id,
        table_id=table_id,
        execution_id=execution_id,
    )

    write_bronze_parquet_to_s3(
        df=df,
        s3_path=s3_path,
        database_name=bronze_database_name,
        table_name=target_name,
        register_catalog=register_catalog,
    )

    logger.info("Ingestão finalizada para fonte: %s", source_name)


def parse_args() -> argparse.Namespace:
    """
    Lê argumentos de linha de comando.

    Exemplos:
    ---------
    Rodar todas as fontes com limite de 100 linhas:
        python src/ingestion/ingest_bronze_batch.py --limit 100

    Rodar apenas uf:
        python src/ingestion/ingest_bronze_batch.py --source uf --limit 100

    Rodar carga completa e registrar no Glue/Athena:
        python src/ingestion/ingest_bronze_batch.py --register-catalog
    """

    parser = argparse.ArgumentParser(
        description="Pipeline Batch de ingestão Bronze da Base dos Dados para S3."
    )

    parser.add_argument(
        "--settings-path",
        default="configs/settings.yaml",
        help="Caminho do arquivo settings.yaml.",
    )

    parser.add_argument(
        "--sources-path",
        default="configs/sources.yaml",
        help="Caminho do arquivo sources.yaml.",
    )

    parser.add_argument(
        "--source",
        default=None,
        help="Nome de uma fonte específica para ingerir. Se omitido, ingere todas.",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limite de linhas para desenvolvimento. Se omitido, carga completa.",
    )

    parser.add_argument(
        "--register-catalog",
        action="store_true",
        help="Se informado, registra as tabelas Bronze no Glue Data Catalog/Athena.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    settings = load_yaml(args.settings_path)
    sources_yaml = load_yaml(args.sources_path)

    sources = sources_yaml.get("sources", {})

    if not sources:
        raise ValueError("Nenhuma fonte encontrada em configs/sources.yaml.")

    brasilia_tz = timezone(timedelta(hours=-3))
    ingestion_timestamp = datetime.now(brasilia_tz) 
    execution_id = ingestion_timestamp.strftime("%Y%m%dT%H%M%SZ")

    logger.info("Iniciando pipeline Bronze Batch.")
    logger.info("Execution ID: %s", execution_id)
    logger.info("Ambiente: %s", settings["project"]["environment"])
    logger.info("Bucket S3: %s", settings["aws"]["bucket"])
    logger.info("Register catalog: %s", args.register_catalog)

    for source_name, source_config in sources.items():
        load_enabled = source_config.get("load_enabled", True)

        if not load_enabled:
            logger.info("Fonte ignorada porque load_enabled=false: %s", source_name)
            continue

        if args.source and source_name != args.source:
            continue

        try:
            ingest_single_source(
                source_name=source_name,
                source_config=source_config,
                settings=settings,
                execution_id=execution_id,
                limit=args.limit,
                register_catalog=args.register_catalog,
            )

        except Exception:
            logger.exception("Erro ao ingerir fonte: %s", source_name)
            raise

    logger.info("Pipeline Bronze Batch finalizado com sucesso.")


if __name__ == "__main__":
    main()