DROP TABLE IF EXISTS {{ silver_database }}.meta_alfabetizacao_municipio;

CREATE TABLE {{ silver_database }}.meta_alfabetizacao_municipio
WITH (
    format = 'PARQUET',
    external_location = 's3://{{ bucket }}/{{ environment }}/{{ silver_path }}/meta_alfabetizacao_municipio/',
    write_compression = 'SNAPPY'
) AS

WITH bronze_padronizada AS (
    SELECT
        TRY_CAST(ano AS INTEGER) AS ano,
        TRY_CAST(id_municipio AS VARCHAR) AS id_municipio,
        TRIM(rede) AS rede,

        TRY_CAST(taxa_alfabetizacao AS DOUBLE) AS taxa_alfabetizacao_pct,

        TRY_CAST(meta_alfabetizacao_2024 AS DOUBLE) AS meta_alfabetizacao_2024_pct,
        TRY_CAST(meta_alfabetizacao_2025 AS DOUBLE) AS meta_alfabetizacao_2025_pct,
        TRY_CAST(meta_alfabetizacao_2026 AS DOUBLE) AS meta_alfabetizacao_2026_pct,
        TRY_CAST(meta_alfabetizacao_2027 AS DOUBLE) AS meta_alfabetizacao_2027_pct,
        TRY_CAST(meta_alfabetizacao_2028 AS DOUBLE) AS meta_alfabetizacao_2028_pct,
        TRY_CAST(meta_alfabetizacao_2029 AS DOUBLE) AS meta_alfabetizacao_2029_pct,
        TRY_CAST(meta_alfabetizacao_2030 AS DOUBLE) AS meta_alfabetizacao_2030_pct,

        TRY_CAST(nivel_alfabetizacao AS INTEGER) AS nivel_alfabetizacao,
        TRY_CAST(percentual_participacao AS DOUBLE) AS percentual_participacao_pct,

        _source_system,
        _source_name,
        _source_dataset_id,
        _source_table_id,
        _ingestion_timestamp_utc,
        _ingestion_date,
        _execution_id

    FROM {{ bronze_database }}.meta_alfabetizacao_municipio
),

bronze_com_flags AS (
    SELECT
        *,

        CASE
            WHEN id_municipio IS NOT NULL AND LENGTH(id_municipio) = 7 THEN true
            ELSE false
        END AS flag_id_municipio_valido,


        CASE
            WHEN taxa_alfabetizacao_pct BETWEEN 0 AND 100 THEN true
            ELSE false
        END AS flag_taxa_alfabetizacao_valida,

        CASE
            WHEN percentual_participacao_pct BETWEEN 0 AND 100 THEN true
            ELSE false
        END AS flag_percentual_participacao_valido,

        CASE
            WHEN nivel_alfabetizacao BETWEEN 0 AND 5 THEN true
            ELSE false
        END AS flag_nivel_alfabetizacao_valido,

        CASE
            WHEN meta_alfabetizacao_2024_pct BETWEEN 0 AND 100 THEN true
            ELSE false
        END AS flag_meta_2024_valida,

        CASE
            WHEN meta_alfabetizacao_2025_pct BETWEEN 0 AND 100
                AND meta_alfabetizacao_2026_pct BETWEEN 0 AND 100
                AND meta_alfabetizacao_2027_pct BETWEEN 0 AND 100
                AND meta_alfabetizacao_2028_pct BETWEEN 0 AND 100
                AND meta_alfabetizacao_2029_pct BETWEEN 0 AND 100
                AND meta_alfabetizacao_2030_pct BETWEEN 0 AND 100
            THEN true
            ELSE false
        END AS flag_metas_2025_2030_validas,

        CONCAT(
            CAST(ano AS VARCHAR),
            '_',
            id_municipio,
            '_',
            rede
        ) AS chave_meta_municipio_ano_rede

    FROM bronze_padronizada
),

bronze_deduplicada AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                ano,
                id_municipio,
                rede
            ORDER BY
                _ingestion_timestamp_utc DESC,
                _execution_id DESC
        ) AS rn

    FROM bronze_com_flags
)

SELECT
    ano,
    id_municipio,
    rede,

    taxa_alfabetizacao_pct,

    meta_alfabetizacao_2024_pct,
    meta_alfabetizacao_2025_pct,
    meta_alfabetizacao_2026_pct,
    meta_alfabetizacao_2027_pct,
    meta_alfabetizacao_2028_pct,
    meta_alfabetizacao_2029_pct,
    meta_alfabetizacao_2030_pct,

    nivel_alfabetizacao,
    percentual_participacao_pct,

    flag_id_municipio_valido,
    flag_taxa_alfabetizacao_valida,
    flag_percentual_participacao_valido,
    flag_nivel_alfabetizacao_valido,
    flag_meta_2024_valida,
    flag_metas_2025_2030_validas,

    chave_meta_municipio_ano_rede,

    _source_system,
    _source_name,
    _source_dataset_id,
    _source_table_id,
    _ingestion_timestamp_utc,
    _ingestion_date,
    _execution_id

FROM bronze_deduplicada
WHERE rn = 1;