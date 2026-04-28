FROM python:3.10-slim

WORKDIR /app

# copiar código
COPY src/system1_classifier /app/system1_classifier
COPY requirements /app/requirements

# instalar dependencias
RUN pip install --no-cache-dir -r /app/requirements/requirements.txt

# opencv deps
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# por defecto NO corre nada (lo ejecutas manual)
CMD ["bash"]