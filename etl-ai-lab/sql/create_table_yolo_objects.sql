CREATE EXTERNAL TABLE IF NOT EXISTS yolo.yolo_objects (
  source_type         STRING,
  source_id           STRING,
  frame_number        BIGINT,    -- era INT
  run_id              STRING,
  detection_id        STRING,
  class_id            BIGINT,    -- era INT
  class_name          STRING,
  confidence          DOUBLE,
  x_min               BIGINT,    -- era INT
  y_min               BIGINT,    -- era INT
  x_max               BIGINT,    -- era INT
  y_max               BIGINT,    -- era INT
  width               BIGINT,    -- era INT
  height              BIGINT,    -- era INT
  area_pixels         BIGINT,    -- era INT
  frame_width         BIGINT,    -- era INT
  frame_height        BIGINT,    -- era INT
  bbox_area_ratio     DOUBLE,
  center_x            DOUBLE,
  center_y            DOUBLE,
  center_x_norm       DOUBLE,
  center_y_norm       DOUBLE,
  position_region     STRING,
  dominant_color_name STRING,
  dom_r               BIGINT,    -- era INT
  dom_g               BIGINT,    -- era INT
  dom_b               BIGINT,    -- era INT
  timestamp_sec       DOUBLE,
  window_10s          BIGINT,    -- era INT
  ingestion_date      STRING,    -- era TIMESTAMP
  source_file         STRING
)
STORED AS PARQUET
LOCATION '{hdfs_location}'
TBLPROPERTIES ('parquet.compression'='SNAPPY')