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

    videos_dir = os.getenv("VIDEOS_DIR", "data/raw/videos")
    staging_dir = os.getenv("STAGING_DIR", "data/staging/videos")
    run_id = os.getenv("RUN_ID", datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8])

    out_dir = os.path.join(staging_dir, f"run_id={run_id}")

    detector = YoloDetector(model_path=model_path, min_conf=min_conf)
    writer = CsvRotatingWriter(out_dir=out_dir, prefix="videos_detections", max_rows_per_file=200_000)

    paths = sorted(
        glob.glob(os.path.join(videos_dir, "*.mp4"))
        + glob.glob(os.path.join(videos_dir, "*.mov"))
        + glob.glob(os.path.join(videos_dir, "*.mkv"))
    )
    if not paths:
        raise RuntimeError(f"No encontré videos en {videos_dir}")

    try:
        for vp in paths:
            cap = cv2.VideoCapture(vp)
            if not cap.isOpened():
                continue

            fps = cap.get(cv2.CAP_PROP_FPS) or 0.0
            frame_number = 0
            source_id = os.path.basename(vp)

            while True:
                ok, frame = cap.read()
                if not ok:
                    break
                frame_number += 1

                # timestamp_sec preferido: POS_MSEC, fallback: frame/fps
                ts_ms = cap.get(cv2.CAP_PROP_POS_MSEC)
                if ts_ms and ts_ms > 0:
                    timestamp_sec = ts_ms / 1000.0
                else:
                    timestamp_sec = (frame_number / fps) if fps else 0.0

                dets = detector.detect(frame)
                frame_h, frame_w = frame.shape[:2]

                for d in dets:
                    row = build_feature_row(
                        run_id=run_id,
                        source_type="video",
                        source_id=source_id,
                        frame_number=frame_number,
                        timestamp_sec=timestamp_sec,
                        class_id=d.class_id,
                        class_name=d.class_name,
                        confidence=d.confidence,
                        bbox_xyxy=(d.x1, d.y1, d.x2, d.y2),
                        frame_shape_hw=(frame_h, frame_w),
                        frame_bgr=frame,
                    )
                    if row:
                        writer.write_row(row)

            cap.release()

    finally:
        writer.close()


if __name__ == "__main__":
    main()