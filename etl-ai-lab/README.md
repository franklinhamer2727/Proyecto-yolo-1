# 🎯 Proyecto Final: YOLO + Hive ETL Pipeline

**Programa de Certificación: Ingeniero de Datos de IA**  
**Curso: Procesos ETL para Cargas de Trabajo de IA**

Pipeline end-to-end que integra **Deep Learning**, **Visión por Computador** y **Big Data** usando YOLOv8, Apache Hive y HDFS.

---

## 🏗️ Arquitectura

```
┌─────────────────────────────┐     CSV      ┌─────────────────────────────┐
│   Sistema 1 - Clasificación │ ──────────► │   Sistema 2 - Batch ETL     │
│                             │             │                             │
│  • YOLOv8n                  │             │  • Extracción CSV           │
│  • OpenCV                   │             │  • Limpieza                 │
│  • 28 atributos/objeto      │             │  • Transformación           │
│  • Imágenes + Videos        │             │  • Deduplicación            │
│  • Genera CSVs staging      │             │  • Windowing 10s            │
│                             │             │  • Carga a Hive             │
└─────────────────────────────┘             └──────────────┬──────────────┘
                                                           │ Parquet
                                                           ▼
                                            ┌─────────────────────────────┐
                                            │   HDFS + Apache Hive        │
                                            │                             │
                                            │  • Tabla yolo_objects       │
                                            │  • Sin duplicados           │
                                            │  • 5 consultas analíticas   │
                                            └─────────────────────────────┘
```

---

## 📋 Requisitos del Sistema

- Ubuntu 24.04
- Python 3.10
- Docker Desktop
- Make
- Git

---

## 📁 Estructura del Proyecto

```
etl-ai-lab/
├── src/
│   ├── system1_classifier/        # Sistema 1: YOLO + CSV
│   │   ├── camera.py
│   │   ├── features.py
│   │   ├── yolo_detector.py
│   │   ├── writer.py
│   │   ├── main_images.py
│   │   ├── main_videos.py
│   │   ├── main_webcam.py
│   │   └── data/
│   │       ├── raw/
│   │       │   ├── imagenes/      # 20+ imágenes propias
│   │       │   └── videos/        # 2+ videos propios
│   │       └── staging/           # CSVs generados
│   └── system2_batch_etl/         # Sistema 2: ETL → Hive
│       ├── main.py
│       ├── extract.py
│       ├── clean.py
│       ├── transform.py
│       ├── dedupe.py
│       ├── windowing.py
│       ├── load_hive.py
│       └── checkpoint.py
├── tests/                         # Pruebas unitarias
├── sql/hive/                      # Scripts SQL Hive
├── docker/                        # Docker Compose
├── requirements/                  # Dependencias
├── Makefile                       # Automatización
├── README.md
└── GUIA_PROYECTO_FINAL_ES.md
```

---

## 🚀 Instalación y Configuración

### 1. Clonar el repositorio

```bash
git clone <url-repositorio>
cd etl-ai-lab
```

### 2. Crear entorno virtual e instalar dependencias

```bash
make venv
make install
```

### 3. Levantar servicios Docker (HDFS + Hive)

```bash
make docker-up
```

Verifica que estén corriendo:
- HDFS UI: http://localhost:9870
- Hive: `jdbc:hive2://localhost:10000`

---

## ▶️ Ejecución

### Sistema 1 — Clasificación con YOLO

Coloca tus imágenes en `src/system1_classifier/data/raw/imagenes/` y videos en `src/system1_classifier/data/raw/videos/`.

```bash
# Procesar imágenes
make run-system1-images

# Procesar videos
make run-system1-videos

# Captura en tiempo real con webcam
make run-system1-webcam

# Procesar todo
make run-system1-all
```

Los CSVs se guardan en `src/system1_classifier/data/staging/`.

### Sistema 2 — Pipeline ETL

```bash
make run-system2
```

Este comando ejecuta en orden: extracción → limpieza → transformación → deduplicación → windowing → carga a Hive.

### Pipeline Completo

```bash
make run-all
```

---

## ✅ Verificación

### Ver datos en Hive

```bash
make hive-shell
```

Dentro de beeline:

```sql
USE yolo;
SHOW TABLES;
SELECT COUNT(*) FROM yolo_objects;
SELECT class_name, COUNT(*) FROM yolo_objects GROUP BY class_name;
```

### Ver archivos en HDFS

```bash
make hdfs-check
```

---

## 🧪 Pruebas

```bash
# Ejecutar todas las pruebas
make test

# Con reporte de cobertura
make test-coverage
```

---

## 🔍 Calidad de Código

```bash
# Linting
make lint

# Formateo
make format
```

---

## 📊 Consultas Analíticas en Hive

```sql
-- 1. Conteo de objetos por clase
SELECT class_name, COUNT(*) AS total
FROM yolo_objects
GROUP BY class_name
ORDER BY total DESC;

-- 2. Número de personas por video
SELECT source_id, COUNT(*) AS total_personas
FROM yolo_objects
WHERE class_name = 'person' AND source_type = 'video'
GROUP BY source_id;

-- 3. Área promedio de bounding boxes por clase
SELECT class_name, ROUND(AVG(area_pixels), 2) AS avg_area
FROM yolo_objects
GROUP BY class_name;

-- 4. Distribución de colores dominantes por clase
SELECT class_name, dominant_color_name, COUNT(*) AS total
FROM yolo_objects
GROUP BY class_name, dominant_color_name
ORDER BY class_name, total DESC;

-- 5. Objetos por ventana de 10 segundos en cada video
SELECT source_id,
       FLOOR(timestamp_sec / 10) * 10 AS ventana_inicio,
       COUNT(*) AS total_objetos
FROM yolo_objects
WHERE source_type = 'video'
GROUP BY source_id, FLOOR(timestamp_sec / 10) * 10
ORDER BY source_id, ventana_inicio;
```

---

## 🐳 Comandos Docker

```bash
make docker-up           # Levantar servicios
make docker-down         # Bajar servicios
make docker-logs         # Ver logs de todos los servicios
make docker-logs-system2 # Ver logs del ETL
make docker-logs-hive    # Ver logs de Hive
```

---

## 🧹 Limpieza

```bash
make clean          # Limpiar archivos temporales
make clean-staging  # Limpiar CSVs de staging
make clean-all      # Limpiar todo incluyendo venv
```

---

## 📷 Tipo de Cámara Utilizada

- **Cámara USB** (webcam) con `device_id=0`

---

## 👤 Autor

**Hamer Jara**  
Programa de Certificación Ingeniero de Datos de IA  
BGS Institute
