import os
from .extract import load_data
from .clean import clean_data
from .transform import transform_data
from .dedupe import deduplicate
from .windowing import apply_windowing
from .load_hive import save_to_parquet, upload_to_hdfs, load_into_hive

# Rutas usando variables de entorno para compatibilidad con Docker
BASE_PATH = os.getenv("STAGING_DIR", "../system1_classifier/data/staging")
OUTPUT_PARQUET = "/app/data/output.parquet"   # ← ruta absoluta dentro del contenedor
HDFS_PATH = "/projects/yolo_objects/hive/"
TABLE = "yolo_objects"

def main():
    print("[ETL] Iniciando pipeline...")

    # 1. Extraccion
    print("[ETL] Extrayendo datos...")
    df = load_data(BASE_PATH)
    print(f"[ETL] Registros extraidos: {len(df)}")

    # 2. Limpieza
    print("[ETL] Limpiando datos...")
    df = clean_data(df)

    # 3. Transformacion
    print("[ETL] Transformando datos...")
    df = transform_data(df)

    # 4. Deduplicacion
    print("[ETL] Eliminando duplicados...")
    df = deduplicate(df)
    print(f"[ETL] Registros unicos: {len(df)}")

    # 5. Windowing (ventanas de 10s para video)
    print("[ETL] Aplicando windowing...")
    df = apply_windowing(df)

    # 6. Guardar parquet local
    print("[ETL] Guardando parquet...")
    save_to_parquet(df, OUTPUT_PARQUET)

    # 7. Subir a HDFS
    print("[ETL] Subiendo a HDFS...")
    hdfs_full_path = upload_to_hdfs(OUTPUT_PARQUET, HDFS_PATH)

    # 8. Crear tabla en Hive  ← orden correcto de argumentos
    print("[ETL] Creando tabla en Hive...")
    load_into_hive(HDFS_PATH, TABLE)

    print("[ETL] Pipeline completado!")


if __name__ == "__main__":
    main()