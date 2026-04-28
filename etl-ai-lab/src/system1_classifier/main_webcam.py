from __future__ import annotations

import os
import uuid
from datetime import datetime

from camera import WebcamReader
from yolo_detector import YoloDetector
from features import build_feature_row
from writer import CsvRotatingWriter


def main():
    # Config por env (docker-friendly)
    model_path = os.getenv("YOLO_MODEL", "yolov8n.pt")
    device = int(os.getenv("WEBCAM_DEVICE", "0"))
    width = int(os.getenv("CAPTURE_W", "640"))
    height = int(os.getenv("CAPTURE_H", "480"))
    min_conf = float(os.getenv("MIN_CONF", "0.25"))

    staging_dir = os.getenv("STAGING_DIR", "data/staging/webcam")
    run_id = os.getenv("RUN_ID", datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8])

    out_dir = os.path.join(staging_dir, f"run_id={run_id}")

    reader = WebcamReader(device=device, width=width, height=height)
    detector = YoloDetector(model_path=model_path, min_conf=min_conf)
    writer = CsvRotatingWriter(out_dir=out_dir, prefix="webcam_detections", max_rows_per_file=50_000)

    try:
        for fr in reader.frames():
            dets = detector.detect(fr.image_bgr)
            frame_h, frame_w = fr.image_bgr.shape[:2]

            for d in dets:
                row = build_feature_row(
                    run_id=run_id,
                    source_type="video",
                    source_id=f"webcam_{device}",
                    frame_number=fr.frame_number,
                    timestamp_sec=fr.timestamp_sec,
                    class_id=d.class_id,
                    class_name=d.class_name,
                    confidence=d.confidence,
                    bbox_xyxy=(d.x1, d.y1, d.x2, d.y2),
                    frame_shape_hw=(frame_h, frame_w),
                    frame_bgr=fr.image_bgr,
                )
                if row:
                    writer.write_row(row)

    except KeyboardInterrupt:
        pass
    finally:
        reader.close()
        writer.close()


if __name__ == "__main__":
    main()