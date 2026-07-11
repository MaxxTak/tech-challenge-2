DROP TABLE IF EXISTS {{ quality_database }}.validation_results;

CREATE TABLE {{ quality_database }}.validation_results
WITH (
    format = 'PARQUET',
    external_location = 's3://{{ bucket }}/{{ environment }}/{{ quality_path }}/validation_results/',
    write_compression = 'SNAPPY'
) AS

WITH checks AS (

    -- =========================================================
    -- SILVER.UF
    -- =========================================================

    SELECT
        'silver' AS layer_name,
        'uf' AS table_name,
        'duplicate_key_ano_sigla_uf_serie_rede' AS check_name,
        'DUPLICIDADE' AS check_type,
        CAST((SELECT COUNT(*) FROM {{ silver_database }}.uf) AS BIGINT) AS total_records,
        CAST((
            SELECT COUNT(*)
            FROM (
                SELECT
                    ano,
                    sigla_uf,
                    serie,
                    rede,
                    COUNT(*) AS qtd
                FROM {{ silver_database }}.uf
                GROUP BY
                    ano,
                    sigla_uf,
                    serie,
                    rede
                HAVING COUNT(*) > 1
            )
        ) AS BIGINT) AS failed_records,
        'Valida duplicidade na chave ano + sigla_uf + serie + rede' AS check_description

    UNION ALL

    SELECT
        'silver',
        'uf',
        'invalid_sigla_uf',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN sigla_uf IS NULL OR LENGTH(sigla_uf) <> 2 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se sigla_uf está preenchida e possui 2 caracteres'
    FROM {{ silver_database }}.uf

    UNION ALL

    SELECT
        'silver',
        'uf',
        'invalid_taxa_alfabetizacao',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN taxa_alfabetizacao_pct IS NULL OR taxa_alfabetizacao_pct NOT BETWEEN 0 AND 100 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se taxa_alfabetizacao_pct está preenchida e entre 0 e 100'
    FROM {{ silver_database }}.uf

    UNION ALL

    SELECT
        'silver',
        'uf',
        'invalid_quality_flag_sigla_uf',
        'CONSISTENCIA_FLAG',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN flag_sigla_uf_valida = false THEN 1 ELSE 0 END) AS BIGINT),
        'Valida a flag técnica flag_sigla_uf_valida'
    FROM {{ silver_database }}.uf

    UNION ALL

    SELECT
        'silver',
        'uf',
        'invalid_quality_flag_taxa_alfabetizacao',
        'CONSISTENCIA_FLAG',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN flag_taxa_alfabetizacao_valida = false THEN 1 ELSE 0 END) AS BIGINT),
        'Valida a flag técnica flag_taxa_alfabetizacao_valida'
    FROM {{ silver_database }}.uf


    -- =========================================================
    -- SILVER.MUNICIPIO
    -- =========================================================

    UNION ALL

    SELECT
        'silver',
        'municipio',
        'duplicate_key_ano_id_municipio_serie_rede',
        'DUPLICIDADE',
        CAST((SELECT COUNT(*) FROM {{ silver_database }}.municipio) AS BIGINT),
        CAST((
            SELECT COUNT(*)
            FROM (
                SELECT
                    ano,
                    id_municipio,
                    serie,
                    rede,
                    COUNT(*) AS qtd
                FROM {{ silver_database }}.municipio
                GROUP BY
                    ano,
                    id_municipio,
                    serie,
                    rede
                HAVING COUNT(*) > 1
            )
        ) AS BIGINT),
        'Valida duplicidade na chave ano + id_municipio + serie + rede'

    UNION ALL

    SELECT
        'silver',
        'municipio',
        'invalid_id_municipio',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN id_municipio IS NULL OR LENGTH(CAST(id_municipio AS VARCHAR)) <> 7 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se id_municipio está preenchido e possui 7 caracteres'
    FROM {{ silver_database }}.municipio

    UNION ALL

    SELECT
        'silver',
        'municipio',
        'invalid_taxa_alfabetizacao',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN taxa_alfabetizacao_pct IS NULL OR taxa_alfabetizacao_pct NOT BETWEEN 0 AND 100 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se taxa_alfabetizacao_pct está preenchida e entre 0 e 100'
    FROM {{ silver_database }}.municipio

    UNION ALL

    SELECT
        'silver',
        'municipio',
        'invalid_quality_flag_id_municipio',
        'CONSISTENCIA_FLAG',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN flag_id_municipio_valido = false THEN 1 ELSE 0 END) AS BIGINT),
        'Valida a flag técnica flag_id_municipio_valido'
    FROM {{ silver_database }}.municipio

    UNION ALL

    SELECT
        'silver',
        'municipio',
        'invalid_quality_flag_taxa_alfabetizacao',
        'CONSISTENCIA_FLAG',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN flag_taxa_alfabetizacao_valida = false THEN 1 ELSE 0 END) AS BIGINT),
        'Valida a flag técnica flag_taxa_alfabetizacao_valida'
    FROM {{ silver_database }}.municipio


    -- =========================================================
    -- SILVER.ALUNOS
    -- =========================================================

    UNION ALL

    SELECT
        'silver',
        'alunos',
        'duplicate_key_ano_municipio_escola_aluno',
        'DUPLICIDADE',
        CAST((SELECT COUNT(*) FROM {{ silver_database }}.alunos) AS BIGINT),
        CAST((
            SELECT COUNT(*)
            FROM (
                SELECT
                    ano,
                    id_municipio,
                    id_escola,
                    id_aluno,
                    COUNT(*) AS qtd
                FROM {{ silver_database }}.alunos
                GROUP BY
                    ano,
                    id_municipio,
                    id_escola,
                    id_aluno
                HAVING COUNT(*) > 1
            )
        ) AS BIGINT),
        'Valida duplicidade na chave ano + id_municipio + id_escola + id_aluno'

    UNION ALL

    SELECT
        'silver',
        'alunos',
        'missing_id_aluno',
        'VALORES_AUSENTES',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN id_aluno IS NULL THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se id_aluno está preenchido'
    FROM {{ silver_database }}.alunos

    UNION ALL

    SELECT
        'silver',
        'alunos',
        'invalid_id_municipio',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN id_municipio IS NULL OR LENGTH(CAST(id_municipio AS VARCHAR)) <> 7 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se id_municipio está preenchido e possui 7 caracteres'
    FROM {{ silver_database }}.alunos

    UNION ALL

    SELECT
        'silver',
        'alunos',
        'invalid_alfabetizado',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN alfabetizado NOT IN (0, 1) OR alfabetizado IS NULL THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se alfabetizado possui valor binário válido'
    FROM {{ silver_database }}.alunos

    UNION ALL

    SELECT
        'silver',
        'alunos',
        'invalid_presenca',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN presenca NOT IN (0, 1) OR presenca IS NULL THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se presenca possui valor binário válido'
    FROM {{ silver_database }}.alunos

    UNION ALL

    SELECT
        'silver',
        'alunos',
        'invalid_preenchimento_caderno',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN preenchimento_caderno NOT IN (0, 1) OR preenchimento_caderno IS NULL THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se preenchimento_caderno possui valor binário válido'
    FROM {{ silver_database }}.alunos

    UNION ALL

    SELECT
        'silver',
        'alunos',
        'invalid_proficiencia',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN proficiencia IS NOT NULL AND proficiencia < 0 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se proficiencia é nula ou maior/igual a zero'
    FROM {{ silver_database }}.alunos

    UNION ALL

    SELECT
        'silver',
        'alunos',
        'invalid_peso_aluno',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN peso_aluno IS NOT NULL AND peso_aluno < 0 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se peso_aluno é nulo ou maior/igual a zero'
    FROM {{ silver_database }}.alunos


    -- =========================================================
    -- SILVER.META_ALFABETIZACAO_BRASIL
    -- =========================================================

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_brasil',
        'duplicate_key_ano_rede',
        'DUPLICIDADE',
        CAST((SELECT COUNT(*) FROM {{ silver_database }}.meta_alfabetizacao_brasil) AS BIGINT),
        CAST((
            SELECT COUNT(*)
            FROM (
                SELECT
                    ano,
                    rede,
                    COUNT(*) AS qtd
                FROM {{ silver_database }}.meta_alfabetizacao_brasil
                GROUP BY
                    ano,
                    rede
                HAVING COUNT(*) > 1
            )
        ) AS BIGINT),
        'Valida duplicidade na chave ano + rede'

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_brasil',
        'invalid_taxa_alfabetizacao',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN taxa_alfabetizacao_pct IS NULL OR taxa_alfabetizacao_pct NOT BETWEEN 0 AND 100 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se taxa_alfabetizacao_pct está preenchida e entre 0 e 100'
    FROM {{ silver_database }}.meta_alfabetizacao_brasil

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_brasil',
        'invalid_percentual_participacao',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN percentual_participacao_pct IS NULL OR percentual_participacao_pct NOT BETWEEN 0 AND 100 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se percentual_participacao_pct está preenchido e entre 0 e 100'
    FROM {{ silver_database }}.meta_alfabetizacao_brasil

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_brasil',
        'invalid_meta_2030',
        'CONSISTENCIA',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN meta_alfabetizacao_2030_pct IS NULL OR meta_alfabetizacao_2030_pct <> 80 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se meta_alfabetizacao_2030_pct está preenchida e igual a 80'
    FROM {{ silver_database }}.meta_alfabetizacao_brasil

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_brasil',
        'invalid_metas_2025_2030',
        'CONSISTENCIA',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN flag_metas_2025_2030_validas = false THEN 1 ELSE 0 END) AS BIGINT),
        'Valida a flag técnica flag_metas_2025_2030_validas'
    FROM {{ silver_database }}.meta_alfabetizacao_brasil


    -- =========================================================
    -- SILVER.META_ALFABETIZACAO_UF
    -- =========================================================

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_uf',
        'duplicate_key_ano_sigla_uf_rede',
        'DUPLICIDADE',
        CAST((SELECT COUNT(*) FROM {{ silver_database }}.meta_alfabetizacao_uf) AS BIGINT),
        CAST((
            SELECT COUNT(*)
            FROM (
                SELECT
                    ano,
                    sigla_uf,
                    rede,
                    COUNT(*) AS qtd
                FROM {{ silver_database }}.meta_alfabetizacao_uf
                GROUP BY
                    ano,
                    sigla_uf,
                    rede
                HAVING COUNT(*) > 1
            )
        ) AS BIGINT),
        'Valida duplicidade na chave ano + sigla_uf + rede'

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_uf',
        'invalid_sigla_uf',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN sigla_uf IS NULL OR LENGTH(sigla_uf) <> 2 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se sigla_uf está preenchida e possui 2 caracteres'
    FROM {{ silver_database }}.meta_alfabetizacao_uf

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_uf',
        'invalid_taxa_alfabetizacao',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN taxa_alfabetizacao_pct IS NULL OR taxa_alfabetizacao_pct NOT BETWEEN 0 AND 100 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se taxa_alfabetizacao_pct está preenchida e entre 0 e 100'
    FROM {{ silver_database }}.meta_alfabetizacao_uf

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_uf',
        'invalid_meta_2030',
        'CONSISTENCIA',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN meta_alfabetizacao_2030_pct IS NULL OR meta_alfabetizacao_2030_pct <> 80 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se meta_alfabetizacao_2030_pct está preenchida e igual a 80'
    FROM {{ silver_database }}.meta_alfabetizacao_uf

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_uf',
        'registro_sem_indicadores',
        'COMPLETUDE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN flag_registro_com_indicadores = false THEN 1 ELSE 0 END) AS BIGINT),
        'Valida registros de UF sem indicadores/metas preenchidos'
    FROM {{ silver_database }}.meta_alfabetizacao_uf


    -- =========================================================
    -- SILVER.META_ALFABETIZACAO_MUNICIPIO
    -- =========================================================

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_municipio',
        'duplicate_key_ano_id_municipio_rede',
        'DUPLICIDADE',
        CAST((SELECT COUNT(*) FROM {{ silver_database }}.meta_alfabetizacao_municipio) AS BIGINT),
        CAST((
            SELECT COUNT(*)
            FROM (
                SELECT
                    ano,
                    id_municipio,
                    rede,
                    COUNT(*) AS qtd
                FROM {{ silver_database }}.meta_alfabetizacao_municipio
                GROUP BY
                    ano,
                    id_municipio,
                    rede
                HAVING COUNT(*) > 1
            )
        ) AS BIGINT),
        'Valida duplicidade na chave ano + id_municipio + rede'

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_municipio',
        'invalid_id_municipio',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN id_municipio IS NULL OR LENGTH(CAST(id_municipio AS VARCHAR)) <> 7 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se id_municipio está preenchido e possui 7 caracteres'
    FROM {{ silver_database }}.meta_alfabetizacao_municipio

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_municipio',
        'invalid_taxa_alfabetizacao',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN taxa_alfabetizacao_pct IS NULL OR taxa_alfabetizacao_pct NOT BETWEEN 0 AND 100 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se taxa_alfabetizacao_pct está preenchida e entre 0 e 100'
    FROM {{ silver_database }}.meta_alfabetizacao_municipio

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_municipio',
        'invalid_percentual_participacao',
        'VALIDADE',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN percentual_participacao_pct IS NULL OR percentual_participacao_pct NOT BETWEEN 0 AND 100 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se percentual_participacao_pct está preenchido e entre 0 e 100'
    FROM {{ silver_database }}.meta_alfabetizacao_municipio

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_municipio',
        'invalid_meta_2030',
        'CONSISTENCIA',
        CAST(COUNT(*) AS BIGINT),
        CAST(SUM(CASE WHEN meta_alfabetizacao_2030_pct IS NULL OR meta_alfabetizacao_2030_pct <> 80 THEN 1 ELSE 0 END) AS BIGINT),
        'Valida se meta_alfabetizacao_2030_pct está preenchida e igual a 80'
    FROM {{ silver_database }}.meta_alfabetizacao_municipio


    -- =========================================================
    -- RELACIONAMENTOS ENTRE TABELAS
    -- =========================================================

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_municipio',
        'municipio_meta_sem_resultado_municipio',
        'CHAVE_RELACIONAMENTO',
        CAST((SELECT COUNT(*) FROM {{ silver_database }}.meta_alfabetizacao_municipio) AS BIGINT),
        CAST((
            SELECT COUNT(*)
            FROM {{ silver_database }}.meta_alfabetizacao_municipio meta
            LEFT JOIN (
                SELECT DISTINCT id_municipio
                FROM {{ silver_database }}.municipio
            ) mun
                ON meta.id_municipio = mun.id_municipio
            WHERE mun.id_municipio IS NULL
        ) AS BIGINT),
        'Valida se todo id_municipio de meta_alfabetizacao_municipio existe em silver.municipio'

    UNION ALL

    SELECT
        'silver',
        'alunos',
        'aluno_sem_resultado_municipio',
        'CHAVE_RELACIONAMENTO',
        CAST((SELECT COUNT(*) FROM {{ silver_database }}.alunos) AS BIGINT),
        CAST((
            SELECT COUNT(*)
            FROM {{ silver_database }}.alunos a
            LEFT JOIN (
                SELECT DISTINCT id_municipio
                FROM {{ silver_database }}.municipio
            ) mun
                ON a.id_municipio = mun.id_municipio
            WHERE mun.id_municipio IS NULL
        ) AS BIGINT),
        'Valida se todo id_municipio de silver.alunos existe em silver.municipio'

    UNION ALL

    SELECT
        'silver',
        'meta_alfabetizacao_uf',
        'uf_meta_sem_resultado_uf',
        'CHAVE_RELACIONAMENTO',
        CAST((SELECT COUNT(*) FROM {{ silver_database }}.meta_alfabetizacao_uf) AS BIGINT),
        CAST((
            SELECT COUNT(*)
            FROM {{ silver_database }}.meta_alfabetizacao_uf meta
            LEFT JOIN (
                SELECT DISTINCT sigla_uf
                FROM {{ silver_database }}.uf
            ) uf
                ON meta.sigla_uf = uf.sigla_uf
            WHERE uf.sigla_uf IS NULL
        ) AS BIGINT),
        'Valida se toda sigla_uf de meta_alfabetizacao_uf existe em silver.uf'
)

SELECT
    layer_name,
    table_name,
    check_name,
    check_type,

    CASE
        WHEN failed_records = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status,

    total_records,
    failed_records,

    CASE
        WHEN total_records > 0
        THEN ROUND(100.0 * failed_records / total_records, 4)
        ELSE 0.0
    END AS failed_pct,

    check_description,

    CAST(current_timestamp AS VARCHAR) AS execution_timestamp_utc

FROM checks;