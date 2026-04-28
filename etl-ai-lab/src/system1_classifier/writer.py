from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from typing import Dict, Optional

from schema import SCHEMA


@dataclass
class CsvRotatingWriter:
    out_dir: str
    prefix: str = "detections"
    max_rows_per_file: int = 50_000  # ajusta según tu laptop
    _file_index: int = 0
    _row_count: int = 0
    _fh: Optional[object] = None
    _writer: Optional[csv.writer] = None

    def __post_init__(self):
        os.makedirs(self.out_dir, exist_ok=True)
        self._open_new_file()

    def _open_new_file(self):
        if self._fh:
            self._fh.close()

        self._file_index += 1
        self._row_count = 0

        path = os.path.join(self.out_dir, f"{self.prefix}_{self._file_index:04d}.csv")
        self._fh = open(path, "w", newline="")
        self._writer = csv.writer(self._fh)

        # header fijo
        self._writer.writerow(SCHEMA.columns)

    def write_row(self, row_dict: Dict[str, object]):
        row = [row_dict.get(c) for c in SCHEMA.columns]
        SCHEMA.validate_row_len(row)

        self._writer.writerow(row)
        self._row_count += 1

        if self._row_count >= self.max_rows_per_file:
            self._open_new_file()

    def close(self):
        try:
            if self._fh:
                self._fh.close()
        except Exception:
            pass