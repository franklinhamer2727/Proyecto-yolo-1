from __future__ import annotations

import glob
import os
import uuid
from datetime import datetime

import cv2

from yolo_detector import YoloDetector
from features import build_feature_row
from writer import CsvRotatingWriter


def main():
    model_path = os.getenv("YOLO_MODEL", "yolov8n.pt")
    min_conf = float(os.getenv("MIN_CONF", "0.25"))

    images_dir = os.getenv("IMAGES_DIR", "data/raw/images")
    staging_dir = os.getenv("STAGING_DIR", "data/staging/images")
    run_id = os.getenv("RUN_ID", datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8])

    out_dir = os.path.join(staging_dir, f"run_id={run_id}")

    detector = YoloDetector(model_path=model_path, min_conf=min_conf)
    writer = CsvRotatingWriter(out_dir=out_dir, prefix="images_detections", max_rows_per_file=200_000)

    paths = sorted(
        glob.glob(os.path.join(images_dir, "*.jpg"))
        + glob.glob(os.path.join(images_dir, "*.jpeg"))
        + glob.glob(os.path.join(images_dir, "*.png"))
    )
    if not paths:
        raise RuntimeError(f"No encontré imágenes en {images_dir}")

    try:
        for p in paths:
            img = cv2.imread(p)
            if img is None:
                continue
            dets = detector.detect(img)
            frame_h, frame_w = img.shape[:2]
            source_id = os.path.basename(p)

            # Para imágenes: frame_number = 0, timestamp_sec = 0
            for d in dets:
                row = build_feature_row(
                    run_id=run_id,
                    source_type="image",
                    source_id=source_id,
                    frame_number=0,
                    timestamp_sec=0.0,
                    class_id=d.class_id,
                    class_name=d.class_name,
                    confidence=d.confidence,
                    bbox_xyxy=(d.x1, d.y1, d.x2, d.y2),
                    frame_shape_hw=(frame_h, frame_w),
                    frame_bgr=img,
                )
                if row:
                    writer.write_row(row)

    finally:
        writer.close()


if __name__ == "__main__":
    main()