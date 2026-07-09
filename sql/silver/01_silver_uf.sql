DROP TABLE IF EXISTS {{ silver_database }}.uf;

CREATE TABLE {{ silver_database }}.uf
WITH (
    format = 'PARQUET',
    external_location = 's3://{{ bucket }}/{{ environment }}/silver/uf/',
    write_compression = 'SNAPPY'
) AS

WITH bronze_padronizada AS (
    SELECT
        TRY_CAST(ano AS INTEGER) AS ano,
        UPPER(TRIM(sigla_uf)) AS sigla_uf,
        TRY_CAST(serie AS INTEGER) AS serie,
        TRY_CAST(rede AS INTEGER) AS rede,

        TRY_CAST(taxa_alfabetizacao AS DOUBLE) AS taxa_alfabetizacao_pct,
        TRY_CAST(media_portugues AS DOUBLE) AS media_portugues,

        TRY_CAST(proporcao_aluno_nivel_0 AS DOUBLE) AS proporcao_aluno_nivel_0_pct,
        TRY_CAST(proporcao_aluno_nivel_1 AS DOUBLE) AS proporcao_aluno_nivel_1_pct,
        TRY_CAST(proporcao_aluno_nivel_2 AS DOUBLE) AS proporcao_aluno_nivel_2_pct,
        TRY_CAST(proporcao_aluno_nivel_3 AS DOUBLE) AS proporcao_aluno_nivel_3_pct,
        TRY_CAST(proporcao_aluno_nivel_4 AS DOUBLE) AS proporcao_aluno_nivel_4_pct,
        TRY_CAST(proporcao_aluno_nivel_5 AS DOUBLE) AS proporcao_aluno_nivel_5_pct,
        TRY_CAST(proporcao_aluno_nivel_6 AS DOUBLE) AS proporcao_aluno_nivel_6_pct,
        TRY_CAST(proporcao_aluno_nivel_7 AS DOUBLE) AS proporcao_aluno_nivel_7_pct,
        TRY_CAST(proporcao_aluno_nivel_8 AS DOUBLE) AS proporcao_aluno_nivel_8_pct,

        _source_system,
        _source_name,
        _source_dataset_id,
        _source_table_id,
        _ingestion_timestamp_utc,
        _ingestion_date,
        _execution_id

    FROM {{ bronze_database }}.uf
),

bronze_com_flags AS (
    SELECT
        *,

        CASE
            WHEN sigla_uf IS NOT NULL AND LENGTH(sigla_uf) = 2 THEN true
            ELSE false
        END AS flag_sigla_uf_valida,

        CASE
            WHEN taxa_alfabetizacao_pct BETWEEN 0 AND 100 THEN true
            ELSE false
        END AS flag_taxa_alfabetizacao_valida,

        CASE
            WHEN
                proporcao_aluno_nivel_0_pct IS NULL
                AND proporcao_aluno_nivel_1_pct IS NULL
                AND proporcao_aluno_nivel_2_pct IS NULL
                AND proporcao_aluno_nivel_3_pct IS NULL
                AND proporcao_aluno_nivel_4_pct IS NULL
                AND proporcao_aluno_nivel_5_pct IS NULL
                AND proporcao_aluno_nivel_6_pct IS NULL
                AND proporcao_aluno_nivel_7_pct IS NULL
                AND proporcao_aluno_nivel_8_pct IS NULL
            THEN false
            ELSE true
        END AS flag_possui_distribuicao_niveis,

        CONCAT(
            CAST(ano AS VARCHAR),
            '_',
            sigla_uf,
            '_',
            CAST(serie AS VARCHAR),
            '_',
            CAST(rede AS VARCHAR)
        ) AS chave_uf_ano_serie_rede

    FROM bronze_padronizada
),

bronze_deduplicada AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                ano,
                sigla_uf,
                serie,
                rede
            ORDER BY
                _ingestion_timestamp_utc DESC,
                _execution_id DESC
        ) AS rn

    FROM bronze_com_flags
)

SELECT
    ano,
    sigla_uf,
    serie,
    rede,

    taxa_alfabetizacao_pct,
    media_portugues,

    proporcao_aluno_nivel_0_pct,
    proporcao_aluno_nivel_1_pct,
    proporcao_aluno_nivel_2_pct,
    proporcao_aluno_nivel_3_pct,
    proporcao_aluno_nivel_4_pct,
    proporcao_aluno_nivel_5_pct,
    proporcao_aluno_nivel_6_pct,
    proporcao_aluno_nivel_7_pct,
    proporcao_aluno_nivel_8_pct,

    flag_sigla_uf_valida,
    flag_taxa_alfabetizacao_valida,
    flag_possui_distribuicao_niveis,

    chave_uf_ano_serie_rede,

    _source_system,
    _source_name,
    _source_dataset_id,
    _source_table_id,
    _ingestion_timestamp_utc,
    _ingestion_date,
    _execution_id

FROM bronze_deduplicada
WHERE rn = 1;