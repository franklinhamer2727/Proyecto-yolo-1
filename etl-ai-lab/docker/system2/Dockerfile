FROM python:3.10-slim

WORKDIR /app

# Instalar dependencias del sistema necesarias para compilar sasl
RUN apt-get update && apt-get install -y \
    curl \
    procps \
    gcc \
    g++ \
    libsasl2-dev \
    libsasl2-modules \
    && rm -rf /var/lib/apt/lists/*

# instalar deps
COPY requirements/requirements.txt /app/requirements/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements/requirements.txt

# copiar código ETL
COPY src/system2_batch_etl /app/system2_batch_etl

CMD ["tail", "-f", "/dev/null"]