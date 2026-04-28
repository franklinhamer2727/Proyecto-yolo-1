import os
import requests
import pandas as pd
from pyhive import hive


def save_to_parquet(df: pd.DataFrame, path: str):
    """Guarda el DataFrame como parquet local."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow", compression=None)
    print(f"[PARQUET] Guardado en: {path}")


def upload_to_hdfs(local_path: str, hdfs_path: str):
    """
    Sube un archivo local a HDFS usando WebHDFS REST API.
    No requiere cliente hdfs instalado.
    """
    hdfs_host = os.getenv("HDFS_HOST", "namenode")
    hdfs_port  = os.getenv("HDFS_WEBHDFS_PORT", "50070")
    filename   = os.path.basename(local_path)

    hdfs_path      = hdfs_path.rstrip("/")
    full_hdfs_path = f"{hdfs_path}/{filename}"
    base_url       = f"http://{hdfs_host}:{hdfs_port}/webhdfs/v1"

    # 1. Crear directorio en HDFS
    mkdir_url = f"{base_url}{hdfs_path}?op=MKDIRS&permission=755"
    r = requests.put(mkdir_url)
    if r.status_code not in (200, 201):
        print(f"[HDFS] Advertencia al crear directorio: {r.status_code} {r.text}")

    # 2. Iniciar subida (WebHDFS devuelve redirect 307)
    create_url = f"{base_url}{full_hdfs_path}?op=CREATE&overwrite=true"
    r = requests.put(create_url, allow_redirects=False)

    if r.status_code == 307:
        upload_url = r.headers["Location"]
        with open(local_path, "rb") as f:
            r2 = requests.put(
                upload_url,
                data=f,
                headers={"Content-Type": "application/octet-stream"},
            )
        if r2.status_code == 201:
            print(f"[HDFS] Subido exitosamente: {full_hdfs_path}")
        else:
            raise RuntimeError(
                f"[HDFS] Error al subir archivo: {r2.status_code} {r2.text}"
            )
    else:
        raise RuntimeError(
            f"[HDFS] No se recibio redirect 307: {r.status_code} {r.text}"
        )

    return full_hdfs_path


def _load_sql(filename: str) -> str:
    """Lee un archivo SQL desde la carpeta /app/sql montada en el contenedor."""
    sql_dir  = os.getenv("SQL_DIR", "/app/sql")
    sql_path = os.path.join(sql_dir, filename)
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"[HIVE] No se encontro el archivo SQL: {sql_path}")
    with open(sql_path, "r") as f:
        return f.read()


def load_into_hive(hdfs_path: str, table: str):
    """
    Crea base de datos y tabla externa en Hive leyendo el DDL
    desde /app/sql/create_table_yolo_objects.sql.
    Sustituye el placeholder {hdfs_location} con la ruta HDFS real.
    """
    hive_host = os.getenv("HIVE_HOST", "hive-server")
    hive_port = int(os.getenv("HIVE_PORT", "10000"))
    namenode  = os.getenv("HDFS_HOST", "namenode")

    hdfs_path     = hdfs_path.rstrip("/")
    hdfs_location = f"hdfs://{namenode}:8020{hdfs_path}"

    # Leer DDL desde archivo SQL y sustituir placeholder de location.
    # El SQL tiene LOCATION '{hdfs_location}' con comillas simples propias de HiveQL,
    # por eso reemplazamos la cadena completa incluyendo las comillas.
    raw_sql = _load_sql("create_table_yolo_objects.sql")
    ddl     = raw_sql.replace("'{hdfs_location}'", f"'{hdfs_location}'")
    ddl     = ddl.strip().rstrip(";") 

    conn   = hive.Connection(host=hive_host, port=hive_port, auth="NONE")
    cursor = conn.cursor()

    try:
        cursor.execute("CREATE DATABASE IF NOT EXISTS yolo")
        print("[HIVE] Base de datos 'yolo' lista")

        cursor.execute(ddl)
        print(f"[HIVE] Tabla yolo.{table} creada apuntando a: {hdfs_location}")

        cursor.execute(f"MSCK REPAIR TABLE yolo.{table}")
        print("[HIVE] MSCK REPAIR completado")

    except Exception as e:
        print(f"[HIVE] Error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()