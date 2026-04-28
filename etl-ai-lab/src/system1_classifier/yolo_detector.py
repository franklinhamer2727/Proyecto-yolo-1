from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

from ultralytics import YOLO


@dataclass(frozen=True)
class Detection:
    class_id: int
    class_name: str
    confidence: float
    x1: int
    y1: int
    x2: int
    y2: int


class YoloDetector:
    def __init__(self, model_path: str, min_conf: float = 0.25):
        self.model = YOLO(model_path)
        self.names = self.model.names
        self.min_conf = float(min_conf)

    def detect(self, image_bgr) -> List[Detection]:
        results = self.model(image_bgr, verbose=False)
        out: List[Detection] = []
        for r in results:
            for b in r.boxes:
                conf = float(b.conf)
                if conf < self.min_conf:
                    continue
                cid = int(b.cls)
                cname = str(self.names.get(cid, str(cid)))
                x1, y1, x2, y2 = map(int, b.xyxy[0].tolist())
                out.append(Detection(cid, cname, conf, x1, y1, x2, y2))
        return out