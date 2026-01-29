import pandas as pd

def assert_not_null(df: pd.DataFrame, cols: list[str]) -> None:
    missing = df[cols].isna().mean()
    bad = missing[missing > 0].sort_values(ascending=False)
    if not bad.empty:
        raise ValueError(f"NOT NULL check failed:\n{bad}")

def assert_unique(df: pd.DataFrame, cols: list[str]) -> None:
    dupes = df.duplicated(subset=cols).sum()
    if dupes > 0:
        raise ValueError(f"UNIQUE check failed on {cols}. Duplicates: {dupes}")

def assert_accepted_values(df: pd.DataFrame, col: str, allowed: set) -> None:
    bad = set(df[col].dropna().unique()) - allowed
    if bad:
        raise ValueError(f"Accepted values check failed for {col}. Bad: {bad}")

def assert_row_count(df: pd.DataFrame, min_rows: int) -> None:
    if len(df) < min_rows:
        raise ValueError(f"Row count check failed. Got {len(df)}, expected >= {min_rows}")
