DROP TABLE IF EXISTS {{ gold_database }}.metas_vs_resultados_municipio;

CREATE TABLE {{ gold_database }}.metas_vs_resultados_municipio
WITH (
    format = 'PARQUET',
    external_location = 's3://{{ bucket }}/{{ environment }}/{{ gold_path }}/metas_vs_resultados_municipio/',
    write_compression = 'SNAPPY'
) AS

WITH resultado_municipal AS (
    SELECT
        ano,
        id_municipio,
        serie,
        rede,
        taxa_alfabetizacao_pct,
        media_portugues,
        flag_id_municipio_valido,
        flag_taxa_alfabetizacao_valida,
        _ingestion_date,
        _execution_id
    FROM {{ silver_database }}.municipio
    WHERE rede = 3
      AND flag_id_municipio_valido = true
      AND flag_taxa_alfabetizacao_valida = true
),

resultado_com_uf AS (
    SELECT
        *,
        SUBSTR(CAST(id_municipio AS VARCHAR), 1, 2) AS codigo_uf,

        -- O id_municipio segue o padrão oficial do IBGE, em que os dois primeiros
        -- dígitos do código de 7 dígitos representam a Unidade da Federação.
        CASE SUBSTR(CAST(id_municipio AS VARCHAR), 1, 2)
            WHEN '11' THEN 'RO'
            WHEN '12' THEN 'AC'
            WHEN '13' THEN 'AM'
            WHEN '14' THEN 'RR'
            WHEN '15' THEN 'PA'
            WHEN '16' THEN 'AP'
            WHEN '17' THEN 'TO'
            WHEN '21' THEN 'MA'
            WHEN '22' THEN 'PI'
            WHEN '23' THEN 'CE'
            WHEN '24' THEN 'RN'
            WHEN '25' THEN 'PB'
            WHEN '26' THEN 'PE'
            WHEN '27' THEN 'AL'
            WHEN '28' THEN 'SE'
            WHEN '29' THEN 'BA'
            WHEN '31' THEN 'MG'
            WHEN '32' THEN 'ES'
            WHEN '33' THEN 'RJ'
            WHEN '35' THEN 'SP'
            WHEN '41' THEN 'PR'
            WHEN '42' THEN 'SC'
            WHEN '43' THEN 'RS'
            WHEN '50' THEN 'MS'
            WHEN '51' THEN 'MT'
            WHEN '52' THEN 'GO'
            WHEN '53' THEN 'DF'
            ELSE NULL
        END AS sigla_uf,

        CASE
            WHEN SUBSTR(CAST(id_municipio AS VARCHAR), 1, 2) IN ('11', '12', '13', '14', '15', '16', '17') THEN 'Norte'
            WHEN SUBSTR(CAST(id_municipio AS VARCHAR), 1, 2) IN ('21', '22', '23', '24', '25', '26', '27', '28', '29') THEN 'Nordeste'
            WHEN SUBSTR(CAST(id_municipio AS VARCHAR), 1, 2) IN ('31', '32', '33', '35') THEN 'Sudeste'
            WHEN SUBSTR(CAST(id_municipio AS VARCHAR), 1, 2) IN ('41', '42', '43') THEN 'Sul'
            WHEN SUBSTR(CAST(id_municipio AS VARCHAR), 1, 2) IN ('50', '51', '52', '53') THEN 'Centro-Oeste'
            ELSE NULL
        END AS regiao

    FROM resultado_municipal
),

metas_municipio AS (
    SELECT
        ano,
        id_municipio,
        rede AS rede_meta,

        taxa_alfabetizacao_pct AS taxa_alfabetizacao_meta_base_pct,

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
        flag_metas_2025_2030_validas

    FROM {{ silver_database }}.meta_alfabetizacao_municipio
    WHERE flag_id_municipio_valido = true
)

SELECT
    r.ano,
    r.id_municipio,
    r.codigo_uf,
    r.sigla_uf,
    r.regiao,
    r.serie,
    r.rede AS rede_resultado,
    m.rede_meta,

    r.taxa_alfabetizacao_pct AS resultado_alfabetizacao_pct,
    r.media_portugues,

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
    END AS meta_ano_resultado_pct,

    CASE
        WHEN r.ano = 2024 THEN ROUND(r.taxa_alfabetizacao_pct - m.meta_alfabetizacao_2024_pct, 2)
        WHEN r.ano = 2025 THEN ROUND(r.taxa_alfabetizacao_pct - m.meta_alfabetizacao_2025_pct, 2)
        WHEN r.ano = 2026 THEN ROUND(r.taxa_alfabetizacao_pct - m.meta_alfabetizacao_2026_pct, 2)
        WHEN r.ano = 2027 THEN ROUND(r.taxa_alfabetizacao_pct - m.meta_alfabetizacao_2027_pct, 2)
        WHEN r.ano = 2028 THEN ROUND(r.taxa_alfabetizacao_pct - m.meta_alfabetizacao_2028_pct, 2)
        WHEN r.ano = 2029 THEN ROUND(r.taxa_alfabetizacao_pct - m.meta_alfabetizacao_2029_pct, 2)
        WHEN r.ano = 2030 THEN ROUND(r.taxa_alfabetizacao_pct - m.meta_alfabetizacao_2030_pct, 2)
        ELSE NULL
    END AS diferenca_meta_ano_pp,

    ROUND(r.taxa_alfabetizacao_pct - m.meta_alfabetizacao_2030_pct, 2) AS diferenca_meta_2030_pp,

    CASE
        WHEN r.ano = 2023 THEN 'SEM_META_ANUAL'
        WHEN
            CASE
                WHEN r.ano = 2024 THEN m.meta_alfabetizacao_2024_pct
                WHEN r.ano = 2025 THEN m.meta_alfabetizacao_2025_pct
                WHEN r.ano = 2026 THEN m.meta_alfabetizacao_2026_pct
                WHEN r.ano = 2027 THEN m.meta_alfabetizacao_2027_pct
                WHEN r.ano = 2028 THEN m.meta_alfabetizacao_2028_pct
                WHEN r.ano = 2029 THEN m.meta_alfabetizacao_2029_pct
                WHEN r.ano = 2030 THEN m.meta_alfabetizacao_2030_pct
                ELSE NULL
            END IS NULL
        THEN 'META_NAO_DISPONIVEL'
        WHEN r.taxa_alfabetizacao_pct >=
            CASE
                WHEN r.ano = 2024 THEN m.meta_alfabetizacao_2024_pct
                WHEN r.ano = 2025 THEN m.meta_alfabetizacao_2025_pct
                WHEN r.ano = 2026 THEN m.meta_alfabetizacao_2026_pct
                WHEN r.ano = 2027 THEN m.meta_alfabetizacao_2027_pct
                WHEN r.ano = 2028 THEN m.meta_alfabetizacao_2028_pct
                WHEN r.ano = 2029 THEN m.meta_alfabetizacao_2029_pct
                WHEN r.ano = 2030 THEN m.meta_alfabetizacao_2030_pct
                ELSE NULL
            END
        THEN 'ATINGIU_META_ANUAL'
        ELSE 'ABAIXO_META_ANUAL'
    END AS status_meta_ano,

    CASE
        WHEN r.taxa_alfabetizacao_pct >= m.meta_alfabetizacao_2030_pct THEN 'ATINGIU_META_2030'
        ELSE 'ABAIXO_META_2030'
    END AS status_meta_2030,

    CASE
        WHEN r.taxa_alfabetizacao_pct < 50 THEN 'ALTA_PRIORIDADE'
        WHEN r.taxa_alfabetizacao_pct < 70 THEN 'MEDIA_PRIORIDADE'
        ELSE 'BAIXA_PRIORIDADE'
    END AS faixa_prioridade_intervencao,

    m.nivel_alfabetizacao,
    m.percentual_participacao_pct,

    CONCAT(
        CAST(r.ano AS VARCHAR),
        '_',
        r.id_municipio,
        '_',
        CAST(r.serie AS VARCHAR),
        '_',
        CAST(r.rede AS VARCHAR)
    ) AS chave_gold_metas_vs_resultados_municipio,

    r._ingestion_date,
    r._execution_id

FROM resultado_com_uf r
LEFT JOIN metas_municipio m
    ON r.ano = m.ano
   AND r.id_municipio = m.id_municipio;