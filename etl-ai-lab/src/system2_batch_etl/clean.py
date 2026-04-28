def clean_data(df):
    df = df.dropna(subset=["detection_id"])

    df = df[df["confidence"].between(0, 1)]

    df = df[df["x_min"] < df["x_max"]]
    df = df[df["y_min"] < df["y_max"]]

    return df