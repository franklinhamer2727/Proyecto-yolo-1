from __future__ import annotations

from dataclasses import dataclass
from typing import List


COLUMNS: List[str] = [
    # A) básica
    "source_type", "source_id", "frame_number", "class_id", "class_name", "confidence",
    # B) bbox
    "x_min", "y_min", "x_max", "y_max",
    "width", "height", "area_pixels",
    "frame_width", "frame_height",
    "bbox_area_ratio",
    "center_x", "center_y",
    "center_x_norm", "center_y_norm",
    "position_region",
    # C) color
    "dominant_color_name", "dom_r", "dom_g", "dom_b",
    # D) video meta
    "timestamp_sec",
    # extra recomendado
    "ingestion_date", "detection_id",
    # opcional pero útil para trazabilidad
    "run_id",
]

NUMERIC_INT = {
    "frame_number", "class_id",
    "x_min", "y_min", "x_max", "y_max",
    "width", "height", "area_pixels",
    "frame_width", "frame_height",
    "dom_r", "dom_g", "dom_b",
}
NUMERIC_FLOAT = {
    "confidence", "bbox_area_ratio",
    "center_x", "center_y",
    "center_x_norm", "center_y_norm",
    "timestamp_sec",
}


@dataclass(frozen=True)
class Schema:
    columns: List[str] = None

    def __post_init__(self):
        object.__setattr__(self, "columns", COLUMNS)

    def validate_row_len(self, row: list) -> None:
        if len(row) != len(self.columns):
            raise ValueError(f"Row tiene {len(row)} cols, schema requiere {len(self.columns)}")


SCHEMA = Schema()