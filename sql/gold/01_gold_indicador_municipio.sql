DROP TABLE IF EXISTS {{ gold_database }}.indicador_municipio;

CREATE TABLE {{ gold_database }}.indicador_municipio
WITH (
    format = 'PARQUET',
    external_location = 's3://{{ bucket }}/{{ environment }}/{{ gold_path }}/indicador_municipio/',
    write_compression = 'SNAPPY'
) AS

WITH municipio_base AS (
    SELECT
        ano,
        id_municipio,
        serie,
        rede,
        taxa_alfabetizacao_pct,
        media_portugues,
        flag_id_municipio_valido,
        flag_taxa_alfabetizacao_valida,
        flag_possui_distribuicao_niveis,
        _ingestion_date,
        _execution_id
    FROM {{ silver_database }}.municipio
    WHERE flag_id_municipio_valido = true
      AND flag_taxa_alfabetizacao_valida = true
),

municipio_com_uf AS (
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

    FROM municipio_base
),

alunos_agregados AS (
    SELECT
        ano,
        id_municipio,
        serie,
        rede,

        COUNT(*) AS qtd_alunos_amostra,

        SUM(CASE WHEN alfabetizado = 1 THEN 1 ELSE 0 END) AS qtd_alunos_alfabetizados_amostra,

        SUM(CASE WHEN presenca = 1 THEN 1 ELSE 0 END) AS qtd_alunos_presentes_amostra,

        AVG(proficiencia) AS media_proficiencia_alunos_amostra,

        SUM(peso_aluno) AS soma_peso_alunos_amostra

    FROM {{ silver_database }}.alunos
    WHERE flag_id_municipio_valido = true
      AND flag_id_aluno_preenchido = true
    GROUP BY
        ano,
        id_municipio,
        serie,
        rede
)

SELECT
    m.ano,
    m.id_municipio,
    m.codigo_uf,
    m.sigla_uf,
    m.regiao,
    m.serie,
    m.rede,

    m.taxa_alfabetizacao_pct,
    m.media_portugues,

    a.qtd_alunos_amostra,
    a.qtd_alunos_alfabetizados_amostra,
    a.qtd_alunos_presentes_amostra,

    CASE
        WHEN a.qtd_alunos_amostra > 0
        THEN ROUND(100.0 * a.qtd_alunos_alfabetizados_amostra / a.qtd_alunos_amostra, 2)
        ELSE NULL
    END AS taxa_alfabetizacao_alunos_amostra_pct,

    CASE
        WHEN a.qtd_alunos_amostra > 0
        THEN ROUND(100.0 * a.qtd_alunos_presentes_amostra / a.qtd_alunos_amostra, 2)
        ELSE NULL
    END AS taxa_presenca_alunos_amostra_pct,

    a.media_proficiencia_alunos_amostra,
    a.soma_peso_alunos_amostra,

    m.flag_possui_distribuicao_niveis,

    CASE
        WHEN m.taxa_alfabetizacao_pct >= 80 THEN 'ACIMA_OU_IGUAL_META_2030'
        ELSE 'ABAIXO_META_2030'
    END AS status_meta_2030,

    80.0 AS meta_nacional_2030_pct,

    ROUND(m.taxa_alfabetizacao_pct - 80.0, 2) AS diferenca_meta_2030_pp,

    CONCAT(
        CAST(m.ano AS VARCHAR),
        '_',
        m.id_municipio,
        '_',
        CAST(m.serie AS VARCHAR),
        '_',
        CAST(m.rede AS VARCHAR)
    ) AS chave_gold_indicador_municipio,

    m._ingestion_date,
    m._execution_id

FROM municipio_com_uf m
LEFT JOIN alunos_agregados a
    ON m.ano = a.ano
   AND m.id_municipio = a.id_municipio
   AND m.serie = a.serie
   AND m.rede = a.rede;