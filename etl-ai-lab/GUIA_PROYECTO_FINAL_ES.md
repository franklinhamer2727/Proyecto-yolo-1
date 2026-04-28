# GUIA_PROYECTO_FINAL_ES.md

# Guía Técnica del Proyecto Final
## Deep Learning + Visión por Computador + Big Data (YOLO + Hive)
**Programa de Certificación: Ingeniero de Datos de IA**

---

## 1. Descripción General

Este proyecto implementa un pipeline end-to-end compuesto por dos sistemas separados:

- **Sistema 1**: Detecta objetos en imágenes y videos usando YOLOv8, extrae 28 atributos por detección y los guarda en archivos CSV locales (staging).
- **Sistema 2**: Lee los CSVs, aplica limpieza, transformación, deduplicación y carga los datos a Apache Hive en formato Parquet, respetando ventanas de 10 segundos para video.

---

## 2. Sistema 1 — Clasificación

### 2.1 Modelo utilizado

- **YOLOv8n** (nano) preentrenado — librería `ultralytics`
- Detecta 80 clases del dataset COCO

### 2.2 Tipo de cámara

- **Cámara USB** (`device_id=0`) para captura en tiempo real
- También soporta archivos de imágenes y videos existentes

### 2.3 Datos capturados

- **Imágenes**: mínimo 20 imágenes propias en `data/raw/imagenes/`
- **Videos**: mínimo 2 videos con personas en `data/raw/videos/`
  - Máximo 20 segundos o 50 MB por video

### 2.4 Atributos extraídos (28 total)

| Grupo | Atributos |
|---|---|
| Básicos | `detection_id`, `source_type`, `source_id`, `frame_number`, `class_id`, `class_name`, `confidence` |
| Bounding Box | `x_min`, `y_min`, `x_max`, `y_max`, `width`, `height`, `area_pixels`, `frame_width`, `frame_height`, `bbox_area_ratio`, `center_x`, `center_y`, `center_x_norm`, `center_y_norm`, `position_region` |
| Color | `dominant_color_name`, `dom_r`, `dom_g`, `dom_b` |
| Tiempo | `timestamp_sec`, `ingestion_date` |

### 2.5 Archivos CSV generados

Los CSVs se guardan en `data/staging/` con la siguiente estructura:

```
data/staging/
├── videos/
│   ├── video_01.csv
│   └── video_02.csv
└── webcam/
    └── webcam_001.csv
```

### 2.6 Restricción importante

El Sistema 1 **NO se conecta a Hive**. Su única responsabilidad es generar los CSVs de staging.

---

## 3. Sistema 2 — Batch ETL

### 3.1 Flujo del pipeline

```
CSVs staging
    │
    ▼
extract.py      → Lee todos los CSVs recursivamente
    │
    ▼
clean.py        → Elimina nulos, valida coordenadas y confidencias
    │
    ▼
transform.py    → Normaliza tipos, genera campos derivados
    │
    ▼
dedupe.py       → Elimina duplicados por detection_id
    │
    ▼
windowing.py    → Agrupa videos en ventanas de 10 segundos
    │
    ▼
load_hive.py    → Guarda Parquet → sube a HDFS → crea tabla Hive
```

### 3.2 Reglas de envío de lotes

**Imágenes:** se envían todas al finalizar el procesamiento completo.

**Videos:** se envían en ventanas de 10 segundos de contenido:

```
Video de 40 segundos → 4 lotes:
  Lote 1: frames con timestamp_sec entre  0 y 10
  Lote 2: frames con timestamp_sec entre 10 y 20
  Lote 3: frames con timestamp_sec entre 20 y 30
  Lote 4: frames con timestamp_sec entre 30 y 40
```

La agrupación se hace con:
```python
FLOOR(timestamp_sec / 10) * 10
```

### 3.3 Estrategia anti-duplicados

Se usa un archivo de **checkpoint** (`checkpoint.json`) que registra qué CSVs ya fueron procesados. Al re-ejecutar el sistema, solo procesa los CSVs nuevos. Adicionalmente, se hace `drop_duplicates` sobre `detection_id` antes de insertar.

### 3.4 Herramientas utilizadas

- `pandas` — manipulación de datos
- `pyarrow` — escritura de Parquet
- `pyhive` — conexión a Hive
- `requests` — WebHDFS para subir archivos a HDFS
- **Sin PySpark** — Python puro como requiere el proyecto

---

## 4. Infraestructura Docker

### Servicios

| Servicio | Puerto | Descripción |
|---|---|---|
| `namenode` | 9870, 8020 | HDFS NameNode |
| `datanode` | — | HDFS DataNode |
| `hive-server` | 10000 | HiveServer2 |
| `system2` | — | Pipeline ETL |

### Comandos principales

```bash
# Levantar todo
make docker-up

# Ver logs del ETL
make docker-logs-system2

# Verificar datos en Hive
make hive-check

# Verificar archivos en HDFS
make hdfs-check
```

---

## 5. Esquema de Tabla en Hive

```sql
CREATE EXTERNAL TABLE yolo_objects (
  source_type         STRING,
  source_id           STRING,
  frame_number        INT,
  class_id            INT,
  class_name          STRING,
  confidence          DOUBLE,
  x_min               INT,
  y_min               INT,
  x_max               INT,
  y_max               INT,
  width               INT,
  height              INT,
  area_pixels         INT,
  frame_width         INT,
  frame_height        INT,
  bbox_area_ratio     DOUBLE,
  center_x            DOUBLE,
  center_y            DOUBLE,
  center_x_norm       DOUBLE,
  center_y_norm       DOUBLE,
  position_region     STRING,
  dominant_color_name STRING,
  dom_r               INT,
  dom_g               INT,
  dom_b               INT,
  timestamp_sec       DOUBLE,
  ingestion_date      STRING,
  detection_id        STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/projects/yolo_objects/hive/';
```

---

## 6. Consultas Analíticas

```sql
-- 1. Conteo de objetos por clase
SELECT class_name, COUNT(*) AS total
FROM yolo_objects
GROUP BY class_name ORDER BY total DESC;

-- 2. Número de personas por video
SELECT source_id, COUNT(*) AS total_personas
FROM yolo_objects
WHERE class_name = 'person' AND source_type = 'video'
GROUP BY source_id;

-- 3. Área promedio de bounding boxes por clase
SELECT class_name, ROUND(AVG(area_pixels), 2) AS avg_area
FROM yolo_objects GROUP BY class_name;

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

## 7. Pruebas Unitarias

```bash
make test
```

Cobertura de pruebas:

| Módulo | Prueba |
|---|---|
| `extract.py` | Lee CSVs correctamente, falla si no hay archivos |
| `clean.py` | Elimina nulos, rechaza coordenadas inválidas |
| `transform.py` | Castea tipos correctamente |
| `dedupe.py` | Elimina duplicados por `detection_id` |
| `windowing.py` | Agrupa correctamente en ventanas de 10s |

---

## 8. Decisiones de Diseño

**¿Por qué WebHDFS en lugar de cliente HDFS?**
El contenedor `system2` usa `python:3.10-slim` que no tiene el cliente HDFS instalado. WebHDFS permite subir archivos via HTTP sin dependencias adicionales.

**¿Por qué Parquet en lugar de CSV en Hive?**
Parquet es columnar, más eficiente para consultas analíticas y es el formato recomendado por el proyecto.

**¿Por qué checkpoint.json para anti-duplicados?**
Permite re-ejecutar el sistema sin reprocesar CSVs ya cargados, lo cual es más eficiente que comparar contra Hive en cada ejecución.

---

## 9. Autor

**Hamer Jara**  
Programa de Certificación Ingeniero de Datos de IA  
BGS Institute — 2026
