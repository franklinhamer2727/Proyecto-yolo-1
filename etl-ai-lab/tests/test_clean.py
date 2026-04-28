"""
Pruebas unitarias para clean.py
"""
import pytest
import pandas as pd
import numpy as np

from system2_batch_etl.clean import clean_data


@pytest.fixture
def df_valido():
    """DataFrame con datos válidos."""
    return pd.DataFrame({
        "detection_id": ["id-1", "id-2", "id-3"],
        "source_type": ["image", "video", "video"],
        "source_id": ["img_01.jpg", "vid_01.mp4", "vid_01.mp4"],
        "frame_number": [0, 1, 2],
        "class_id": [0, 1, 2],
        "class_name": ["person", "car", "dog"],
        "confidence": [0.9, 0.85, 0.7],
        "x_min": [10, 20, 30],
        "y_min": [10, 20, 30],
        "x_max": [100, 200, 300],
        "y_max": [100, 200, 300],
        "width": [90, 180, 270],
        "height": [90, 180, 270],
        "area_pixels": [8100, 32400, 72900],
        "frame_width": [640, 640, 640],
        "frame_height": [480, 480, 480],
        "bbox_area_ratio": [0.05, 0.2, 0.45],
        "center_x": [55.0, 110.0, 165.0],
        "center_y": [55.0, 110.0, 165.0],
        "center_x_norm": [0.08, 0.17, 0.25],
        "center_y_norm": [0.11, 0.22, 0.34],
        "position_region": ["top-left", "middle-center", "bottom-right"],
        "dominant_color_name": ["red", "blue", "green"],
        "dom_r": [200, 0, 0],
        "dom_g": [0, 0, 200],
        "dom_b": [0, 200, 0],
        "timestamp_sec": [0.0, 0.033, 0.066],
        "ingestion_date": ["2026-01-01 00:00:00"] * 3,
    })


def test_clean_data_retorna_dataframe(df_valido):
    """Debe retornar un DataFrame."""
    result = clean_data(df_valido)
    assert isinstance(result, pd.DataFrame)


def test_clean_data_elimina_nulos(df_valido):
    """Debe eliminar filas con valores nulos en columnas críticas."""
    df_valido.loc[0, "class_name"] = None
    df_valido.loc[1, "confidence"] = np.nan
    result = clean_data(df_valido)
    assert result["class_name"].notna().all()
    assert result["confidence"].notna().all()


def test_clean_data_elimina_confidencia_invalida(df_valido):
    """Debe eliminar filas con confidence fuera de [0, 1]."""
    df_valido.loc[0, "confidence"] = 1.5
    df_valido.loc[1, "confidence"] = -0.1
    result = clean_data(df_valido)
    assert (result["confidence"] >= 0).all()
    assert (result["confidence"] <= 1).all()


def test_clean_data_elimina_coordenadas_invalidas(df_valido):
    """Debe eliminar filas con coordenadas negativas."""
    df_valido.loc[0, "x_min"] = -10
    df_valido.loc[1, "y_max"] = -5
    result = clean_data(df_valido)
    assert (result["x_min"] >= 0).all()
    assert (result["y_max"] >= 0).all()


def test_clean_data_elimina_bbox_invertido(df_valido):
    """Debe eliminar filas donde x_min >= x_max o y_min >= y_max."""
    df_valido.loc[0, "x_min"] = 200
    df_valido.loc[0, "x_max"] = 100
    result = clean_data(df_valido)
    assert (result["x_min"] < result["x_max"]).all()


def test_clean_data_no_modifica_datos_validos(df_valido):
    """No debe eliminar filas con datos válidos."""
    result = clean_data(df_valido)
    assert len(result) == len(df_valido)