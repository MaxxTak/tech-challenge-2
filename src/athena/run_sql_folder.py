import argparse
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any

import awswrangler as wr
import boto3
from jinja2 import StrictUndefined, Template

# Permite importar módulos a partir da raiz do projeto
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from src.utils.config import load_yaml


# Logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# Configurações e templates

def get_template_params(settings: dict[str, Any]) -> dict[str, Any]:
    """
    Monta os parâmetros que podem ser usados nos arquivos SQL.

    Exemplos de uso no SQL:
        {{ bucket }}
        {{ environment }}
        {{ bronze_database }}
        {{ silver_database }}
        {{ silver_path }}
    """

    return {
        "project_name": settings["project"]["name"],
        "environment": settings["project"]["environment"],

        "bucket": settings["aws"]["bucket"],
        "aws_region": settings["aws"]["region"],

        "bronze_database": settings["databases"]["bronze"],
        "silver_database": settings["databases"]["silver"],
        "gold_database": settings["databases"]["gold"],
        "quality_database": settings["databases"]["quality"],
        "monitoring_database": settings["databases"]["monitoring"],

        "bronze_path": settings["paths"]["bronze"],
        "silver_path": settings["paths"]["silver"],
        "gold_path": settings["paths"]["gold"],
        "quality_path": settings["paths"]["quality"],
        "monitoring_path": settings["paths"]["monitoring"],

        "athena_workgroup": settings["athena"]["workgroup"],
        "athena_query_output_location": settings["athena"]["query_output_location"],
    }


def render_sql_template(sql_text: str, params: dict[str, Any]) -> str:
    """
    Renderiza um SQL usando Jinja2.

    O StrictUndefined faz o código falhar se existir uma variável no SQL
    que não foi definida no settings/template_params.

    Isso evita enviar para o Athena um SQL com placeholder quebrado.
    """

    template = Template(sql_text, undefined=StrictUndefined)
    return template.render(**params)


# Leitura e separação dos arquivos SQL

def list_sql_files(folder: str | Path, recursive: bool = False) -> list[Path]:
    """
    Lista arquivos .sql de uma pasta, em ordem alfabética.

    A ordem dos arquivos importa. Por isso usamos prefixos como:
        01_silver_uf.sql
        02_silver_municipio.sql
        03_silver_meta_uf.sql
    """

    folder = Path(folder)

    if not folder.exists():
        raise FileNotFoundError(f"Pasta SQL não encontrada: {folder}")

    if not folder.is_dir():
        raise NotADirectoryError(f"O caminho informado não é uma pasta: {folder}")

    pattern = "**/*.sql" if recursive else "*.sql"
    sql_files = sorted(folder.glob(pattern))

    if not sql_files:
        raise FileNotFoundError(f"Nenhum arquivo .sql encontrado em: {folder}")

    return sql_files


def split_sql_statements(sql: str) -> list[str]:
    """
    Divide um arquivo SQL em múltiplos statements.

    O Athena executa um comando por vez via start_query_execution.
    Então este arquivo:

        DROP TABLE IF EXISTS silver.uf;

        CREATE TABLE silver.uf
        WITH (...) AS
        SELECT ...
        ;

    vira dois statements.

    Observação:
    Evite usar ponto-e-vírgula dentro de strings SQL.
    """

    statements: list[str] = []
    current_lines: list[str] = []

    for line in sql.splitlines():
        stripped_line = line.strip()

        # Ignora linhas totalmente comentadas
        if stripped_line.startswith("--"):
            continue

        # Ignora linhas vazias fora de um statement
        if not stripped_line and not current_lines:
            continue

        current_lines.append(line)

        if stripped_line.endswith(";"):
            statement = "\n".join(current_lines).strip()
            statement = statement.rstrip(";").strip()

            if statement:
                statements.append(statement)

            current_lines = []

    remaining_statement = "\n".join(current_lines).strip()

    if remaining_statement:
        statements.append(remaining_statement)

    return statements


# Limpeza de paths S3

def extract_external_locations(sql: str) -> list[str]:
    """
    Extrai paths S3 definidos em external_location nos scripts CTAS.

    Exemplo:
        external_location = 's3://bucket/dev/silver/uf/'

    Retorna:
        ['s3://bucket/dev/silver/uf/']
    """

    pattern = r"external_location\s*=\s*'([^']+)'"
    return re.findall(pattern, sql, flags=re.IGNORECASE)


def validate_safe_s3_path_to_delete(s3_path: str, settings: dict[str, Any]) -> None:
    """
    Valida se o path S3 parece seguro para limpeza.

    Esta proteção evita apagar acidentalmente a Bronze ou a raiz do bucket.
    O runner deve limpar apenas camadas derivadas, como Silver, Gold,
    Quality e Monitoring.
    """

    bucket = settings["aws"]["bucket"]
    environment = settings["project"]["environment"]

    allowed_prefixes = [
        f"s3://{bucket}/{environment}/{settings['paths']['silver']}/",
        f"s3://{bucket}/{environment}/{settings['paths']['gold']}/",
        f"s3://{bucket}/{environment}/{settings['paths']['quality']}/",
        f"s3://{bucket}/{environment}/{settings['paths']['monitoring']}/",
    ]

    if not any(s3_path.startswith(prefix) for prefix in allowed_prefixes):
        raise ValueError(
            "Path S3 bloqueado para limpeza por segurança. "
            f"Path recebido: {s3_path}. "
            f"Prefixes permitidos: {allowed_prefixes}"
        )


def delete_s3_prefix(s3_path: str, settings: dict[str, Any]) -> None:
    """
    Remove todos os objetos dentro de um prefixo S3.

    Use apenas para camadas derivadas, como Silver e Gold.
    Nunca use para apagar Bronze automaticamente.
    """

    validate_safe_s3_path_to_delete(s3_path=s3_path, settings=settings)

    logger.info("Verificando objetos para limpeza em: %s", s3_path)

    objects = wr.s3.list_objects(s3_path)

    if not objects:
        logger.info("Nenhum objeto encontrado para limpar em: %s", s3_path)
        return

    logger.info("Removendo %s objeto(s) em: %s", len(objects), s3_path)
    wr.s3.delete_objects(objects)
    logger.info("Limpeza concluída em: %s", s3_path)


def clean_external_locations(sql: str, settings: dict[str, Any]) -> None:
    """
    Encontra os external_location do SQL renderizado e limpa os paths antes
    da execução.

    Isso é útil antes de executar DROP TABLE + CREATE TABLE AS SELECT no Athena,
    porque o DROP TABLE remove o metadado do catálogo, mas não remove
    necessariamente os arquivos antigos do S3.
    """

    external_locations = extract_external_locations(sql)

    if not external_locations:
        logger.info("Nenhum external_location encontrado para limpeza.")
        return

    for s3_path in external_locations:
        delete_s3_prefix(s3_path=s3_path, settings=settings)


# Execução Athena

def create_athena_client(region_name: str):
    """
    Cria cliente boto3 para Athena.
    """

    return boto3.client("athena", region_name=region_name)


def start_query_execution(
    athena_client,
    query: str,
    workgroup: str,
    query_output_location: str,
) -> str:
    """
    Inicia execução de uma query no Athena.
    """

    response = athena_client.start_query_execution(
        QueryString=query,
        WorkGroup=workgroup,
        ResultConfiguration={
            "OutputLocation": query_output_location,
        },
    )

    query_execution_id = response["QueryExecutionId"]

    logger.info("Query iniciada: %s", query_execution_id)

    return query_execution_id


def get_query_status(athena_client, query_execution_id: str) -> dict[str, Any]:
    """
    Consulta status de uma query no Athena.
    """

    response = athena_client.get_query_execution(
        QueryExecutionId=query_execution_id,
    )

    return response["QueryExecution"]


def wait_for_query_completion(
    athena_client,
    query_execution_id: str,
    poll_interval_seconds: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    """
    Aguarda a query finalizar.

    Estados finais:
        SUCCEEDED
        FAILED
        CANCELLED
    """

    terminal_states = {"SUCCEEDED", "FAILED", "CANCELLED"}
    started_at = time.time()

    while True:
        query_execution = get_query_status(
            athena_client=athena_client,
            query_execution_id=query_execution_id,
        )

        status = query_execution["Status"]
        state = status["State"]

        if state in terminal_states:
            if state == "SUCCEEDED":
                statistics = query_execution.get("Statistics", {})
                data_scanned_bytes = statistics.get("DataScannedInBytes")
                execution_time_ms = statistics.get("EngineExecutionTimeInMillis")

                logger.info(
                    "Query finalizada com sucesso: %s | scanned_bytes=%s | execution_time_ms=%s",
                    query_execution_id,
                    data_scanned_bytes,
                    execution_time_ms,
                )

                return query_execution

            reason = status.get("StateChangeReason", "Motivo não informado.")

            raise RuntimeError(
                "Query Athena falhou. "
                f"query_execution_id={query_execution_id} | "
                f"state={state} | "
                f"reason={reason}"
            )

        elapsed_seconds = time.time() - started_at

        if elapsed_seconds > timeout_seconds:
            raise TimeoutError(
                "Timeout aguardando query Athena. "
                f"query_execution_id={query_execution_id} | "
                f"timeout_seconds={timeout_seconds}"
            )

        logger.info(
            "Aguardando query: %s | state=%s",
            query_execution_id,
            state,
        )

        time.sleep(poll_interval_seconds)


def run_query(
    athena_client,
    query: str,
    workgroup: str,
    query_output_location: str,
    poll_interval_seconds: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    """
    Executa uma query e aguarda sua finalização.
    """

    query_execution_id = start_query_execution(
        athena_client=athena_client,
        query=query,
        workgroup=workgroup,
        query_output_location=query_output_location,
    )

    query_execution = wait_for_query_completion(
        athena_client=athena_client,
        query_execution_id=query_execution_id,
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=timeout_seconds,
    )

    return query_execution


# Orquestração dos arquivos SQL

def run_sql_file(
    sql_file: Path,
    athena_client,
    settings: dict[str, Any],
    template_params: dict[str, Any],
    clean_output_paths: bool,
    dry_run: bool,
    poll_interval_seconds: int,
    timeout_seconds: int,
) -> None:
    """
    Renderiza e executa um arquivo SQL no Athena.
    """

    logger.info("=" * 100)
    logger.info("Processando arquivo SQL: %s", sql_file)

    raw_sql = sql_file.read_text(encoding="utf-8")
    rendered_sql = render_sql_template(
        sql_text=raw_sql,
        params=template_params,
    )

    if clean_output_paths:
        clean_external_locations(
            sql=rendered_sql,
            settings=settings,
        )

    statements = split_sql_statements(rendered_sql)

    logger.info(
        "Arquivo %s possui %s statement(s).",
        sql_file.name,
        len(statements),
    )

    for index, statement in enumerate(statements, start=1):
        logger.info(
            "Executando statement %s/%s do arquivo %s",
            index,
            len(statements),
            sql_file.name,
        )

        if dry_run:
            logger.info("DRY RUN ativado. SQL renderizado:")
            logger.info("\n%s", statement)
            continue

        query_execution = run_query(
            athena_client=athena_client,
            query=statement,
            workgroup=settings["athena"]["workgroup"],
            query_output_location=settings["athena"]["query_output_location"],
            poll_interval_seconds=poll_interval_seconds,
            timeout_seconds=timeout_seconds,
        )

        statistics = query_execution.get("Statistics", {})

        logger.info(
            "Statement concluído: arquivo=%s | statement=%s/%s | query_id=%s | scanned_bytes=%s",
            sql_file.name,
            index,
            len(statements),
            query_execution["QueryExecutionId"],
            statistics.get("DataScannedInBytes"),
        )


# CLI

def parse_args() -> argparse.Namespace:
    """
    Lê argumentos de linha de comando.
    """

    parser = argparse.ArgumentParser(
        description="Executa arquivos SQL no Athena com suporte a templates Jinja."
    )

    parser.add_argument(
        "--folder",
        required=True,
        help="Pasta contendo arquivos .sql. Exemplo: sql/silver",
    )

    parser.add_argument(
        "--settings-path",
        default="configs/settings.yaml",
        help="Caminho do arquivo settings.yaml.",
    )

    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Busca arquivos .sql em subpastas.",
    )

    parser.add_argument(
        "--clean-output-paths",
        action="store_true",
        help=(
            "Limpa os paths S3 encontrados em external_location antes de executar. "
            "Use para recriar tabelas CTAS em Silver/Gold."
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Renderiza e mostra os SQLs, mas não executa no Athena.",
    )

    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=2,
        help="Intervalo de checagem do status da query.",
    )

    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=300,
        help="Tempo máximo de espera por query.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    settings = load_yaml(args.settings_path)
    template_params = get_template_params(settings)

    sql_files = list_sql_files(
        folder=args.folder,
        recursive=args.recursive,
    )

    logger.info("Iniciando execução de SQLs no Athena.")
    logger.info("Pasta SQL: %s", args.folder)
    logger.info("Arquivos encontrados: %s", [str(file) for file in sql_files])
    logger.info("Ambiente: %s", template_params["environment"])
    logger.info("Bucket: %s", template_params["bucket"])
    logger.info("Workgroup Athena: %s", template_params["athena_workgroup"])
    logger.info(
        "Query output location: %s",
        template_params["athena_query_output_location"],
    )
    logger.info("Clean output paths: %s", args.clean_output_paths)
    logger.info("Dry run: %s", args.dry_run)

    athena_client = create_athena_client(
        region_name=settings["aws"]["region"],
    )

    for sql_file in sql_files:
        run_sql_file(
            sql_file=sql_file,
            athena_client=athena_client,
            settings=settings,
            template_params=template_params,
            clean_output_paths=args.clean_output_paths,
            dry_run=args.dry_run,
            poll_interval_seconds=args.poll_interval_seconds,
            timeout_seconds=args.timeout_seconds,
        )

    logger.info("Execução de SQLs finalizada com sucesso.")


if __name__ == "__main__":
    main()