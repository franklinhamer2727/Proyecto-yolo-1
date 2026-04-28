"""
Pruebas unitarias para extract.py
"""
import os
import pytest
import pandas as pd
import tempfile

from system2_batch_etl.extract import find_csv_files, load_data


@pytest.fixture
def temp_staging_dir():
    """Crea un directorio temporal con CSVs de prueba."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Crear subcarpetas
        videos_dir = os.path.join(tmpdir, "videos")
        webcam_dir = os.path.join(tmpdir, "webcam")
        os.makedirs(videos_dir)
        os.makedirs(webcam_dir)

        # CSV de prueba para videos
        df_video = pd.DataFrame({
            "detection_id": ["abc-1", "abc-2"],
            "source_type": ["video", "video"],
            "source_id": ["video_01.mp4", "video_01.mp4"],
            "frame_number": [0, 1],
            "class_name": ["person", "car"],
            "confidence": [0.9, 0.85],
            "timestamp_sec": [0.0, 0.033],
        })
        df_video.to_csv(os.path.join(videos_dir, "video_01.csv"), index=False)

        # CSV de prueba para webcam
        df_webcam = pd.DataFrame({
            "detection_id": ["xyz-1"],
            "source_type": ["video"],
            "source_id": ["webcam_001.mp4"],
            "frame_number": [5],
            "class_name": ["person"],
            "confidence": [0.95],
            "timestamp_sec": [0.166],
        })
        df_webcam.to_csv(os.path.join(webcam_dir, "webcam_001.csv"), index=False)

        yield tmpdir


def test_find_csv_files_encuentra_todos(temp_staging_dir):
    """Debe encontrar todos los CSVs recursivamente."""
    files = find_csv_files(temp_staging_dir)
    assert len(files) == 2


def test_find_csv_files_solo_csv(temp_staging_dir):
    """Solo debe retornar archivos .csv."""
    # Crear archivo no-csv
    with open(os.path.join(temp_staging_dir, "notas.txt"), "w") as f:
        f.write("texto")
    files = find_csv_files(temp_staging_dir)
    assert all(f.endswith(".csv") for f in files)


def test_find_csv_files_directorio_vacio():
    """Debe retornar lista vacía si no hay CSVs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        files = find_csv_files(tmpdir)
        assert files == []


def test_load_data_carga_correctamente(temp_staging_dir):
    """Debe concatenar todos los CSVs en un DataFrame."""
    df = load_data(temp_staging_dir)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3  # 2 del video + 1 del webcam
    assert "__source_file" in df.columns


def test_load_data_falla_sin_csvs():
    """Debe lanzar RuntimeError si no hay CSVs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        with pytest.raises(RuntimeError, match="No CSV found"):
            load_data(tmpdir)


def test_load_data_agrega_source_file(temp_staging_dir):
    """Debe agregar columna __source_file a cada registro."""
    df = load_data(temp_staging_dir)
    assert "__source_file" in df.columns
    assert df["__source_file"].notna().all()