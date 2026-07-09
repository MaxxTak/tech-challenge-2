tech-challenge-fase2/
│
├── configs/
│   ├── sources.yaml
│   └── settings.yaml
│
├── src/
│   ├── ingestion/
│   │   ├── ingest_bronze_batch.py
│   │   └── ingest_streaming_simulated.py
│   │
│   ├── athena/
│   │   ├── athena_client.py
│   │   └── run_sql_folder.py
│   │
│   ├── quality/
│   │   └── run_quality_checks.py
│   │
│   └── utils/
│       ├── logger.py
│       └── paths.py
│
├── sql/
│   ├── bronze/
│   │   └── create_external_tables.sql
│   │
│   ├── silver/
│   │   ├── 01_silver_uf.sql
│   │   ├── 02_silver_municipio.sql
│   │   ├── 03_silver_meta_brasil.sql
│   │   ├── 04_silver_meta_uf.sql
│   │   ├── 05_silver_meta_municipio.sql
│   │   └── 06_silver_meta_brasil.sql
│   │
│   └── gold/
│       ├── 01_gold_metas_vs_resultados_municipio.sql
│       ├── 02_gold_indicador_por_uf.sql
│       └── 03_gold_evolucao_temporal.sql
│
├── docs/
│   ├── architecture.md
│   ├── finops.md
│   └── monitoring.md
│
│
├── README.md
├── requirements.txt
└── .gitignore