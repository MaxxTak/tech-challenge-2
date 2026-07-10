DROP TABLE IF EXISTS {{ gold_database }}.evolucao_alfabetizacao_uf;

CREATE TABLE {{ gold_database }}.evolucao_alfabetizacao_uf
WITH (
    format = 'PARQUET',
    external_location = 's3://{{ bucket }}/{{ environment }}/{{ gold_path }}/evolucao_alfabetizacao_uf/',
    write_compression = 'SNAPPY'
) AS

WITH resultado_uf AS (
    SELECT
        ano,
        sigla_uf,
        serie,
        rede,
        taxa_alfabetizacao_pct,
        media_portugues,
        flag_sigla_uf_valida,
        flag_taxa_alfabetizacao_valida,
        flag_possui_distribuicao_niveis,
        _ingestion_date,
        _execution_id
    FROM {{ silver_database }}.uf
    WHERE rede = 5
      AND flag_sigla_uf_valida = true
      AND flag_taxa_alfabetizacao_valida = true
),

resultado_uf_com_regiao AS (
    SELECT
        *,

        CASE
            WHEN sigla_uf IN ('RO', 'AC', 'AM', 'RR', 'PA', 'AP', 'TO') THEN 'Norte'
            WHEN sigla_uf IN ('MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA') THEN 'Nordeste'
            WHEN sigla_uf IN ('MG', 'ES', 'RJ', 'SP') THEN 'Sudeste'
            WHEN sigla_uf IN ('PR', 'SC', 'RS') THEN 'Sul'
            WHEN sigla_uf IN ('MS', 'MT', 'GO', 'DF') THEN 'Centro-Oeste'
            ELSE NULL
        END AS regiao

    FROM resultado_uf
),

metas_uf AS (
    SELECT
        ano,
        sigla_uf,
        rede AS rede_meta,

        taxa_alfabetizacao_pct AS taxa_alfabetizacao_meta_base_pct,

        meta_alfabetizacao_2024_pct,
        meta_alfabetizacao_2025_pct,
        meta_alfabetizacao_2026_pct,
        meta_alfabetizacao_2027_pct,
        meta_alfabetizacao_2028_pct,
        meta_alfabetizacao_2029_pct,
        meta_alfabetizacao_2030_pct,

        percentual_participacao_pct,

        flag_sigla_uf_valida,
        flag_taxa_alfabetizacao_valida,
        flag_percentual_participacao_valido,
        flag_meta_2024_valida,
        flag_metas_2025_2030_validas,
        flag_registro_com_indicadores

    FROM {{ silver_database }}.meta_alfabetizacao_uf
    WHERE flag_sigla_uf_valida = true
),

meta_brasil AS (
    SELECT
        ano,
        rede AS rede_brasil,
        taxa_alfabetizacao_pct AS taxa_alfabetizacao_brasil_pct,
        meta_alfabetizacao_2030_pct AS meta_brasil_2030_pct,
        percentual_participacao_pct AS percentual_participacao_brasil_pct
    FROM {{ silver_database }}.meta_alfabetizacao_brasil
),

base_integrada AS (
    SELECT
        r.ano,
        r.sigla_uf,
        r.regiao,
        r.serie,
        r.rede AS rede_resultado,
        m.rede_meta,

        r.taxa_alfabetizacao_pct AS resultado_alfabetizacao_uf_pct,
        r.media_portugues,

        b.taxa_alfabetizacao_brasil_pct,

        m.meta_alfabetizacao_2024_pct,
        m.meta_alfabetizacao_2025_pct,
        m.meta_alfabetizacao_2026_pct,
        m.meta_alfabetizacao_2027_pct,
        m.meta_alfabetizacao_2028_pct,
        m.meta_alfabetizacao_2029_pct,
        m.meta_alfabetizacao_2030_pct,

        CASE
            WHEN r.ano = 2024 THEN m.meta_alfabetizacao_2024_pct
            WHEN r.ano = 2025 THEN m.meta_alfabetizacao_2025_pct
            WHEN r.ano = 2026 THEN m.meta_alfabetizacao_2026_pct
            WHEN r.ano = 2027 THEN m.meta_alfabetizacao_2027_pct
            WHEN r.ano = 2028 THEN m.meta_alfabetizacao_2028_pct
            WHEN r.ano = 2029 THEN m.meta_alfabetizacao_2029_pct
            WHEN r.ano = 2030 THEN m.meta_alfabetizacao_2030_pct
            ELSE NULL
        END AS meta_ano_resultado_uf_pct,

        m.percentual_participacao_pct AS percentual_participacao_uf_pct,
        b.percentual_participacao_brasil_pct,

        r.flag_possui_distribuicao_niveis,
        m.flag_registro_com_indicadores,

        r._ingestion_date,
        r._execution_id

    FROM resultado_uf_com_regiao r
    LEFT JOIN metas_uf m
        ON r.ano = m.ano
       AND r.sigla_uf = m.sigla_uf
    LEFT JOIN meta_brasil b
        ON r.ano = b.ano
),

base_com_evolucao AS (
    SELECT
        *,

        LAG(resultado_alfabetizacao_uf_pct) OVER (
            PARTITION BY sigla_uf, rede_resultado
            ORDER BY ano
        ) AS resultado_alfabetizacao_uf_ano_anterior_pct,

        ROUND(
            resultado_alfabetizacao_uf_pct
            - LAG(resultado_alfabetizacao_uf_pct) OVER (
                PARTITION BY sigla_uf, rede_resultado
                ORDER BY ano
            ),
            2
        ) AS variacao_alfabetizacao_uf_pp,

        RANK() OVER (
            PARTITION BY ano
            ORDER BY resultado_alfabetizacao_uf_pct DESC
        ) AS ranking_uf_no_ano

    FROM base_integrada
)

SELECT
    ano,
    sigla_uf,
    regiao,
    serie,
    rede_resultado,
    rede_meta,

    resultado_alfabetizacao_uf_pct,
    resultado_alfabetizacao_uf_ano_anterior_pct,
    variacao_alfabetizacao_uf_pp,

    taxa_alfabetizacao_brasil_pct,
    ROUND(resultado_alfabetizacao_uf_pct - taxa_alfabetizacao_brasil_pct, 2) AS diferenca_brasil_pp,

    meta_ano_resultado_uf_pct,

    CASE
        WHEN meta_ano_resultado_uf_pct IS NULL THEN NULL
        ELSE ROUND(resultado_alfabetizacao_uf_pct - meta_ano_resultado_uf_pct, 2)
    END AS diferenca_meta_ano_pp,

    meta_alfabetizacao_2030_pct,
    ROUND(resultado_alfabetizacao_uf_pct - meta_alfabetizacao_2030_pct, 2) AS diferenca_meta_2030_pp,

    CASE
        WHEN meta_ano_resultado_uf_pct IS NULL THEN 'SEM_META_ANUAL'
        WHEN resultado_alfabetizacao_uf_pct >= meta_ano_resultado_uf_pct THEN 'ATINGIU_META_ANUAL'
        ELSE 'ABAIXO_META_ANUAL'
    END AS status_meta_ano,

    CASE
        WHEN resultado_alfabetizacao_uf_pct >= meta_alfabetizacao_2030_pct THEN 'ATINGIU_META_2030'
        ELSE 'ABAIXO_META_2030'
    END AS status_meta_2030,

    ranking_uf_no_ano,

    percentual_participacao_uf_pct,
    percentual_participacao_brasil_pct,

    flag_possui_distribuicao_niveis,
    flag_registro_com_indicadores,

    CONCAT(
        CAST(ano AS VARCHAR),
        '_',
        sigla_uf,
        '_',
        CAST(rede_resultado AS VARCHAR)
    ) AS chave_gold_evolucao_alfabetizacao_uf,

    _ingestion_date,
    _execution_id

FROM base_com_evolucao;