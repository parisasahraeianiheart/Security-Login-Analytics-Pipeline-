import pandas as pd
import numpy as np
from src.config import STAGING_DIR
from src.utils_logging import get_logger
from src.utils_quality import assert_accepted_values, assert_unique

logger = get_logger()

def transform(dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    users = dfs["users"].copy()
    ip_rep = dfs["ip_rep"].copy()
    events = dfs["events"].copy()

    # types
    events["timestamp"] = pd.to_datetime(events["timestamp"], errors="coerce")

    # basic cleaning
    events = events.drop_duplicates(subset=["event_id"])
    users = users.drop_duplicates(subset=["user_id"])
    ip_rep = ip_rep.drop_duplicates(subset=["ip"])

    # quality: accepted values
    assert_accepted_values(events, "device_type", {"mobile","desktop","tablet"})
    assert_accepted_values(events, "browser", {"chrome","firefox","safari","edge"})

    # missing handling (simple, interview-ready)
    num_cols = ["session_duration","bytes_sent","bytes_received","failed_logins"]
    for c in num_cols:
        events[c] = pd.to_numeric(events[c], errors="coerce")
        events[c] = events[c].fillna(events[c].median())

    events["is_vpn"] = events["is_vpn"].fillna(0).astype(int)
    events["success"] = events["success"].fillna(0).astype(int)
    events["label"] = events["label"].fillna(0).astype(int)

    # join keys checks
    assert_unique(users, ["user_id"])
    assert_unique(ip_rep, ["ip"])

    # write staging (parquet is better than csv)
    users.to_parquet(STAGING_DIR / "stg_users.parquet", index=False)
    ip_rep.to_parquet(STAGING_DIR / "stg_ip_rep.parquet", index=False)
    events.to_parquet(STAGING_DIR / "stg_events.parquet", index=False)

    logger.info(f"Wrote staging parquet to {STAGING_DIR}")
    return {"stg_users": users, "stg_ip_rep": ip_rep, "stg_events": events}

if __name__ == "__main__":
    # quick local test
    from src.etl_extract import extract
    transform(extract())