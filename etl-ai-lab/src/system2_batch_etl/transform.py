from .schema import NUMERIC_INT, NUMERIC_FLOAT

def transform_data(df):
    for col in NUMERIC_INT:
        if col in df.columns:
            df[col] = df[col].astype("int64", errors="ignore")

    for col in NUMERIC_FLOAT:
        if col in df.columns:
            df[col] = df[col].astype("float64", errors="ignore")

    return df