import os
import pandas as pd

def find_csv_files(base_path: str):
    csv_files = []
    for root, _, files in os.walk(base_path):
        for f in files:
            if f.endswith(".csv"):
                csv_files.append(os.path.join(root, f))
    return csv_files


def load_data(base_path: str):
    files = find_csv_files(base_path)
    if not files:
        raise RuntimeError(f"No CSV found in {base_path}")

    dfs = []
    for f in files:
        df = pd.read_csv(f)
        df["__source_file"] = f
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)