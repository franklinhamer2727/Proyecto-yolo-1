# =============================================================================
# Makefile - Proyecto Final: YOLO + Hive ETL Pipeline
# Programa de Certificacion Ingeniero de Datos de IA
# =============================================================================

PYTHON      := python3.10
VENV        := .venv
PIP         := $(VENV)/bin/pip
PYTEST      := $(VENV)/bin/pytest
PYLINT      := $(VENV)/bin/pylint
BLACK       := $(VENV)/bin/black

REQ_SYSTEM1 := src/system1_classifier/requirements.txt
REQ_SYSTEM2 := requirements/requirements.txt

SRC_SYSTEM1 := src/system1_classifier
SRC_SYSTEM2 := src/system2_batch_etl
TESTS       := tests/

STAGING_DIR := src/system1_classifier/data/staging
DATA_IMGS   := src/system1_classifier/data/raw/imagenes
DATA_VIDS   := src/system1_classifier/data/raw/videos

.PHONY: all venv install lint format test run-system1-images run-system1-videos \
        run-system1-webcam run-system2 docker-up docker-down docker-logs \
        docker-build clean help

# =============================================================================
# DEFAULT
# =============================================================================
all: venv install lint test
	@echo "✅ Todo listo!"

# =============================================================================
# ENTORNO VIRTUAL
# =============================================================================
venv:
	@echo "🔧 Creando entorno virtual con $(PYTHON)..."
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	@echo "✅ Entorno virtual creado en $(VENV)/"

# =============================================================================
# INSTALACION DE DEPENDENCIAS
# =============================================================================
install: venv
	@echo "📦 Instalando dependencias de system1..."
	$(PIP) install -r $(REQ_SYSTEM1)
	@echo "📦 Instalando dependencias de system2..."
	$(PIP) install -r $(REQ_SYSTEM2)
	@echo "✅ Dependencias instaladas"

# =============================================================================
# LINTING
# =============================================================================
lint:
	@echo "🔍 Ejecutando pylint en system1..."
	$(PYLINT) $(SRC_SYSTEM1) --disable=C0114,C0115,C0116 --fail-under=7.0 || true
	@echo "🔍 Ejecutando pylint en system2..."
	$(PYLINT) $(SRC_SYSTEM2) --disable=C0114,C0115,C0116 --fail-under=7.0 || true
	@echo "✅ Linting completado"

# =============================================================================
# FORMATEO
# =============================================================================
format:
	@echo "🎨 Formateando código con black..."
	$(BLACK) $(SRC_SYSTEM1) $(SRC_SYSTEM2) $(TESTS)
	@echo "✅ Formateo completado"

# =============================================================================
# TESTS
# =============================================================================
test:
	@echo "🧪 Ejecutando pruebas unitarias..."
	$(PYTEST) $(TESTS) -v --tb=short
	@echo "✅ Pruebas completadas"

test-coverage:
	@echo "🧪 Ejecutando pruebas con coverage..."
	$(PYTEST) $(TESTS) -v --cov=$(SRC_SYSTEM2) --cov-report=term-missing
	@echo "✅ Coverage completado"

# =============================================================================
# SISTEMA 1 - CLASIFICACION
# =============================================================================
run-system1-images:
	@echo "🖼️  Procesando imagenes con YOLO..."
	$(VENV)/bin/python -m system1_classifier.main_images \
		--carpeta $(DATA_IMGS) \
		--output $(STAGING_DIR)
	@echo "✅ Imagenes procesadas. CSVs en $(STAGING_DIR)"

run-system1-videos:
	@echo "🎥 Procesando videos con YOLO..."
	$(VENV)/bin/python -m system1_classifier.main_videos \
		--carpeta $(DATA_VIDS) \
		--output $(STAGING_DIR)
	@echo "✅ Videos procesados. CSVs en $(STAGING_DIR)"

run-system1-webcam:
	@echo "📷 Iniciando captura webcam con YOLO..."
	$(VENV)/bin/python -m system1_classifier.main_webcam \
		--device 0 \
		--output $(STAGING_DIR)
	@echo "✅ Captura webcam completada"

run-system1-all:
	@echo "🚀 Procesando imagenes y videos con YOLO..."
	$(MAKE) run-system1-images
	$(MAKE) run-system1-videos
	@echo "✅ Sistema 1 completado"

# =============================================================================
# SISTEMA 2 - BATCH ETL
# =============================================================================
run-system2:
	@echo "⚙️  Ejecutando pipeline ETL..."
	$(VENV)/bin/python -m system2_batch_etl.main
	@echo "✅ Pipeline ETL completado"

# =============================================================================
# DOCKER
# =============================================================================
docker-build:
	@echo "🐳 Construyendo imagenes Docker..."
	cd docker && docker compose build --no-cache
	@echo "✅ Build completado"

docker-up:
	@echo "🐳 Levantando servicios Docker..."
	cd docker && docker compose up -d
	@echo "✅ Servicios levantados"
	@echo "📊 HDFS UI:  http://localhost:9870"
	@echo "🐝 Hive:     jdbc:hive2://localhost:10000"

docker-down:
	@echo "🐳 Bajando servicios Docker..."
	cd docker && docker compose down
	@echo "✅ Servicios detenidos"

docker-down-volumes:
	@echo "🐳 Bajando servicios y eliminando volúmenes..."
	cd docker && docker compose down -v
	@echo "✅ Servicios y volúmenes eliminados"

docker-logs:
	cd docker && docker compose logs -f

docker-logs-system2:
	docker logs -f system2

docker-logs-hive:
	docker logs -f hive-server

# =============================================================================
# PIPELINE COMPLETO
# =============================================================================
run-all: docker-up
	@echo "⏳ Esperando que los servicios estén listos..."
	sleep 30
	$(MAKE) run-system1-all
	@echo "⏳ Esperando que system2 procese los datos..."
	sleep 10
	$(MAKE) run-system2
	@echo "✅ Pipeline completo ejecutado"

# =============================================================================
# VERIFICACION HIVE
# =============================================================================
hive-shell:
	@echo "🐝 Abriendo shell de Hive..."
	docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000

hive-check:
	@echo "🐝 Verificando datos en Hive..."
	docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 \
		-e "SHOW DATABASES; USE yolo; SHOW TABLES; SELECT COUNT(*) FROM yolo_objects;"

hdfs-check:
	@echo "📁 Verificando archivos en HDFS..."
	docker exec -it namenode hdfs dfs -ls /projects/yolo_objects/hive/

# =============================================================================
# LIMPIEZA
# =============================================================================
clean:
	@echo "🧹 Limpiando archivos temporales..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Limpieza completada"

clean-staging:
	@echo "🧹 Limpiando CSVs de staging..."
	find $(STAGING_DIR) -name "*.csv" -delete
	@echo "✅ Staging limpiado"

clean-all: clean clean-staging
	@echo "🧹 Eliminando entorno virtual..."
	rm -rf $(VENV)
	@echo "✅ Todo limpiado"

# =============================================================================
# AYUDA
# =============================================================================
help:
	@echo ""
	@echo "=========================================="
	@echo "  Proyecto Final YOLO + Hive ETL Pipeline"
	@echo "=========================================="
	@echo ""
	@echo "CONFIGURACION:"
	@echo "  make venv              Crear entorno virtual"
	@echo "  make install           Instalar dependencias"
	@echo ""
	@echo "CALIDAD DE CODIGO:"
	@echo "  make lint              Ejecutar pylint"
	@echo "  make format            Formatear con black"
	@echo "  make test              Ejecutar pruebas unitarias"
	@echo "  make test-coverage     Pruebas con cobertura"
	@echo ""
	@echo "SISTEMA 1 - CLASIFICACION:"
	@echo "  make run-system1-images   Procesar imagenes"
	@echo "  make run-system1-videos   Procesar videos"
	@echo "  make run-system1-webcam   Captura webcam"
	@echo "  make run-system1-all      Procesar todo"
	@echo ""
	@echo "SISTEMA 2 - ETL:"
	@echo "  make run-system2       Ejecutar pipeline ETL"
	@echo ""
	@echo "DOCKER:"
	@echo "  make docker-build      Construir imagenes"
	@echo "  make docker-up         Levantar servicios"
	@echo "  make docker-down       Bajar servicios"
	@echo "  make docker-logs       Ver logs"
	@echo ""
	@echo "VERIFICACION:"
	@echo "  make hive-shell        Abrir shell Hive"
	@echo "  make hive-check        Verificar datos en Hive"
	@echo "  make hdfs-check        Verificar archivos HDFS"
	@echo ""
	@echo "LIMPIEZA:"
	@echo "  make clean             Limpiar temporales"
	@echo "  make clean-staging     Limpiar CSVs staging"
	@echo "  make clean-all         Limpiar todo"
	@echo ""
