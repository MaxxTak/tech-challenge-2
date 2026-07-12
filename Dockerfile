FROM python:3.11-slim

WORKDIR /app

# Ensure Python outputs are sent straight to terminal without buffering
ENV PYTHONUNBUFFERED=1

# System dependencies required by some packages (e.g. cryptography, pyarrow)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Install only the dependencies required by stream_source_bronze.py:
#   - kafka-python  : Kafka consumer
#   - pymysql       : MySQL/MariaDB driver
#   - python-dotenv : .env file loading
#   - awswrangler   : S3 parquet read/write (wraps boto3 + pandas + pyarrow)
#   - boto3         : AWS SDK (also pulled in by awswrangler, pinned for reproducibility)
#   - pandas        : DataFrame construction
#   - pyarrow       : Parquet serialisation backend used by awswrangler
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    pymysql==1.2.0 \
    python-dotenv==1.2.2 \
    awswrangler==3.16.1 \
    boto3==1.43.34 \
    pandas==2.3.3 \
    pyarrow==24.0.0

# Copy only the script that needs to run (context is the project root)
COPY src/ingestion/stream_source_bronze.py .

# Run the service
CMD ["python", "stream_source_bronze.py"]
