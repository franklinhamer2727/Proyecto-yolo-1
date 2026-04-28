from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Dict, Tuple

import cv2
import numpy as np


def sanitize_bbox(x1, y1, x2, y2, frame_w, frame_h):
    x1 = max(0, min(int(x1), frame_w - 1))
    y1 = max(0, min(int(y1), frame_h - 1))
    x2 = max(0, min(int(x2), frame_w))
    y2 = max(0, min(int(y2), frame_h))
    w = max(0, x2 - x1)
    h = max(0, y2 - y1)
    return x1, y1, x2, y2, w, h


def position_region(center_x_norm: float, center_y_norm: float) -> str:
    row = "top" if center_y_norm < 1/3 else ("middle" if center_y_norm < 2/3 else "bottom")
    col = "left" if center_x_norm < 1/3 else ("center" if center_x_norm < 2/3 else "right")
    return f"{row}-{col}"


def dominant_color_rgb_name(roi_bgr: np.ndarray) -> Tuple[str, int, int, int]:
    if roi_bgr is None or roi_bgr.size == 0:
        return ("unknown", 0, 0, 0)
    small = cv2.resize(roi_bgr, (64, 64), interpolation=cv2.INTER_AREA)
    b, g, r = np.mean(small.reshape(-1, 3), axis=0).astype(int).tolist()

    name = "unknown"
    if r > 200 and g > 200 and b > 200:
        name = "white"
    elif r < 60 and g < 60 and b < 60:
        name = "black"
    elif r > g and r > b:
        name = "red"
    elif g > r and g > b:
        name = "green"
    elif b > r and b > g:
        name = "blue"
    elif r > 150 and g > 150 and b < 120:
        name = "yellow"

    return name, r, g, b  # RGB


def make_detection_id(run_id: str, source_id: str, frame_number: int, class_id: int, x1: int, y1: int, x2: int, y2: int) -> str:
    raw = f"{run_id}|{source_id}|{frame_number}|{class_id}|{x1}|{y1}|{x2}|{y2}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def ingestion_date_utc() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def build_feature_row(
    *,
    run_id: str,
    source_type: str,
    source_id: str,
    frame_number: int,
    timestamp_sec: float,
    class_id: int,
    class_name: str,
    confidence: float,
    bbox_xyxy: Tuple[int, int, int, int],
    frame_shape_hw: Tuple[int, int],
    frame_bgr,
) -> Dict[str, object]:
    frame_h, frame_w = frame_shape_hw
    x1, y1, x2, y2 = bbox_xyxy
    x1, y1, x2, y2, w, h = sanitize_bbox(x1, y1, x2, y2, frame_w, frame_h)
    if w == 0 or h == 0:
        return {}

    area = w * h
    frame_area = frame_w * frame_h
    cx = x1 + w / 2
    cy = y1 + h / 2
    cxn = cx / frame_w if frame_w else 0.0
    cyn = cy / frame_h if frame_h else 0.0

    roi = frame_bgr[y1:y2, x1:x2]
    dom_name, dom_r, dom_g, dom_b = dominant_color_rgb_name(roi)
    det_id = make_detection_id(run_id, source_id, frame_number, class_id, x1, y1, x2, y2)

    return {
        "source_type": source_type,
        "source_id": source_id,
        "frame_number": int(frame_number),
        "class_id": int(class_id),
        "class_name": str(class_name),
        "confidence": float(confidence),

        "x_min": int(x1), "y_min": int(y1), "x_max": int(x2), "y_max": int(y2),
        "width": int(w), "height": int(h), "area_pixels": int(area),

        "frame_width": int(frame_w), "frame_height": int(frame_h),
        "bbox_area_ratio": float(area / frame_area) if frame_area else 0.0,

        "center_x": float(cx), "center_y": float(cy),
        "center_x_norm": float(cxn), "center_y_norm": float(cyn),
        "position_region": position_region(cxn, cyn),

        "dominant_color_name": dom_name,
        "dom_r": int(dom_r), "dom_g": int(dom_g), "dom_b": int(dom_b),

        "timestamp_sec": float(timestamp_sec) if timestamp_sec is not None else None,

        "ingestion_date": ingestion_date_utc(),
        "detection_id": det_id,
        "run_id": run_id,
    }