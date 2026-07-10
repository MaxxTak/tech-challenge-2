DROP TABLE IF EXISTS {{ silver_database }}.alunos;

CREATE TABLE {{ silver_database }}.alunos
WITH (
    format = 'PARQUET',
    external_location = 's3://{{ bucket }}/{{ environment }}/{{ silver_path }}/alunos/',
    write_compression = 'SNAPPY'
) AS

WITH bronze_padronizada AS (
    SELECT
        TRY_CAST(ano AS INTEGER) AS ano,
        TRY_CAST(id_municipio AS VARCHAR) AS id_municipio,
        TRY_CAST(id_escola AS VARCHAR) AS id_escola,
        TRY_CAST(id_aluno AS VARCHAR) AS id_aluno,

        TRY_CAST(caderno AS INTEGER) AS caderno,
        TRY_CAST(serie AS INTEGER) AS serie,
        TRY_CAST(rede AS INTEGER) AS rede,
        TRY_CAST(presenca AS INTEGER) AS presenca,
        TRY_CAST(preenchimento_caderno AS INTEGER) AS preenchimento_caderno,
        TRY_CAST(alfabetizado AS INTEGER) AS alfabetizado,

        TRY_CAST(proficiencia AS DOUBLE) AS proficiencia,
        TRY_CAST(peso_aluno AS DOUBLE) AS peso_aluno,

        _source_system,
        _source_name,
        _source_dataset_id,
        _source_table_id,
        _ingestion_timestamp_utc,
        _ingestion_date,
        _execution_id

    FROM {{ bronze_database }}.alunos
),

bronze_com_flags AS (
    SELECT
        *,

        CASE
            WHEN id_municipio IS NOT NULL AND LENGTH(id_municipio) = 7 THEN true
            ELSE false
        END AS flag_id_municipio_valido,

        CASE
            WHEN id_aluno IS NOT NULL THEN true
            ELSE false
        END AS flag_id_aluno_preenchido,

        CASE
            WHEN presenca IN (0, 1) THEN true
            ELSE false
        END AS flag_presenca_valida,

        CASE
            WHEN preenchimento_caderno IN (0, 1) THEN true
            ELSE false
        END AS flag_preenchimento_caderno_valido,

        CASE
            WHEN alfabetizado IN (0, 1) THEN true
            ELSE false
        END AS flag_alfabetizado_valido,

        CASE
            WHEN proficiencia IS NULL OR proficiencia >= 0 THEN true
            ELSE false
        END AS flag_proficiencia_valida,

        CASE
            WHEN peso_aluno IS NULL OR peso_aluno >= 0 THEN true
            ELSE false
        END AS flag_peso_aluno_valido,

        CONCAT(
            CAST(ano AS VARCHAR),
            '_',
            id_municipio,
            '_',
            id_escola,
            '_',
            id_aluno
        ) AS chave_aluno_ano_municipio_escola

    FROM bronze_padronizada
),

bronze_deduplicada AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                ano,
                id_municipio,
                id_escola,
                id_aluno
            ORDER BY
                _ingestion_timestamp_utc DESC,
                _execution_id DESC
        ) AS rn

    FROM bronze_com_flags
)

SELECT
    ano,
    id_municipio,
    id_escola,
    id_aluno,

    caderno,
    serie,
    rede,
    presenca,
    preenchimento_caderno,
    alfabetizado,

    proficiencia,
    peso_aluno,

    flag_id_municipio_valido,
    flag_id_aluno_preenchido,
    flag_presenca_valida,
    flag_preenchimento_caderno_valido,
    flag_alfabetizado_valido,
    flag_proficiencia_valida,
    flag_peso_aluno_valido,

    chave_aluno_ano_municipio_escola,

    _source_system,
    _source_name,
    _source_dataset_id,
    _source_table_id,
    _ingestion_timestamp_utc,
    _ingestion_date,
    _execution_id

FROM bronze_deduplicada
WHERE rn = 1;