def apply_windowing(df):
    df["window_10s"] = (df["timestamp_sec"] // 10).astype(int)
    return df