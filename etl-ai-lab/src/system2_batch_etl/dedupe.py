def deduplicate(df):
    return df.drop_duplicates(subset=["detection_id"])