from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterator, Optional, Tuple

import cv2


@dataclass
class Frame:
    image_bgr: "cv2.Mat"
    frame_number: int
    timestamp_sec: float  # desde inicio de captura (aprox)


class WebcamReader:
    """
    Webcam reader robusto:
    - usa CAP_PROP_POS_MSEC si existe, sino monotonic.
    """

    def __init__(self, device: int = 0, width: int = 640, height: int = 480, fps: Optional[int] = None):
        self.device = device
        self.cap = cv2.VideoCapture(device)
        if not self.cap.isOpened():
            raise RuntimeError(f"No se pudo abrir webcam device={device}")

        self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, int(width))
        self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, int(height))
        if fps is not None:
            self.cap.set(cv2.CAP_PROP_FPS, int(fps))

        self._frame_number = 0
        self._t0 = time.monotonic()

    def __iter__(self) -> Iterator[Frame]:
        return self.frames()

    def frames(self) -> Iterator[Frame]:
        while True:
            ok, img = self.cap.read()
            if not ok:
                break
            self._frame_number += 1

            ts_ms = self.cap.get(cv2.CAP_PROP_POS_MSEC)
            if ts_ms and ts_ms > 0:
                ts = ts_ms / 1000.0
            else:
                ts = time.monotonic() - self._t0

            yield Frame(image_bgr=img, frame_number=self._frame_number, timestamp_sec=float(ts))

    def close(self) -> None:
        try:
            self.cap.release()
        except Exception:
            pass