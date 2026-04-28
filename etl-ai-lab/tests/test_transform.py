"""
Pruebas unitarias para transform.py
"""
import pytest
import pandas as pd

from system2_batch_etl.transform import transform_data


@pytest.fixture
def df_base():
    """DataFrame base para pruebas de transformación."""
    return pd.DataFrame({
        "detection_id": ["id-1", "id-2"],
        "source_type": ["image", "video"],
        "source_id": ["img_01.jpg", "vid_01.mp4"],
        "frame_number": ["0", "1"],           # strings que deben castearse
        "class_id": ["0", "1"],               # strings que deben castearse
        "class_name": ["person", "car"],
        "confidence": ["0.9", "0.85"],        # strings que deben castearse
        "x_min": ["10", "20"],
        "y_min": ["10", "20"],
        "x_max": ["100", "200"],
        "y_max": ["100", "200"],
        "width": ["90", "180"],
        "height": ["90", "180"],
        "area_pixels": ["8100", "32400"],
        "frame_width": ["640", "640"],
        "frame_height": ["480", "480"],
        "bbox_area_ratio": ["0.05", "0.2"],
        "center_x": ["55.0", "110.0"],
        "center_y": ["55.0", "110.0"],
        "center_x_norm": ["0.08", "0.17"],
        "center_y_norm": ["0.11", "0.22"],
        "position_region": ["top-left", "middle-center"],
        "dominant_color_name": ["red", "blue"],
        "dom_r": ["200", "0"],
        "dom_g": ["0", "0"],
        "dom_b": ["0", "200"],
        "timestamp_sec": ["0.0", "0.033"],
        "ingestion_date": ["2026-01-01 00:00:00", "2026-01-01 00:00:01"],
    })


def test_transform_data_retorna_dataframe(df_base):
    """Debe retornar un DataFrame."""
    result = transform_data(df_base)
    assert isinstance(result, pd.DataFrame)


def test_transform_castea_frame_number(df_base):
    """frame_number debe ser entero."""
    result = transform_data(df_base)
    assert result["frame_number"].dtype in ["int32", "int64"]


def test_transform_castea_class_id(df_base):
    """class_id debe ser entero."""
    result = transform_data(df_base)
    assert result["class_id"].dtype in ["int32", "int64"]


def test_transform_castea_confidence(df_base):
    """confidence debe ser float."""
    result = transform_data(df_base)
    assert result["confidence"].dtype in ["float32", "float64"]


def test_transform_castea_coordenadas(df_base):
    """Coordenadas x_min, y_min, x_max, y_max deben ser enteros."""
    result = transform_data(df_base)
    for col in ["x_min", "y_min", "x_max", "y_max"]:
        assert result[col].dtype in ["int32", "int64"]


def test_transform_castea_timestamp(df_base):
    """timestamp_sec debe ser float."""
    result = transform_data(df_base)
    assert result["timestamp_sec"].dtype in ["float32", "float64"]


def test_transform_no_pierde_filas(df_base):
    """No debe perder filas durante la transformación."""
    result = transform_data(df_base)
    assert len(result) == len(df_base)