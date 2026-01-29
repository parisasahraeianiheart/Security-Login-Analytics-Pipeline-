import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from src.config import RAW_DIR
from src.utils_logging import get_logger

logger = get_logger()

def main(n_users: int = 2000, n_events: int = 20000, seed: int = 42) -> None:
    rng = np.random.default_rng(seed)

    users = pd.DataFrame({
        "user_id": np.arange(1, n_users + 1),
        "country": rng.choice(["US","CA","UK","DE","FR","IN"], size=n_users, p=[0.45,0.1,0.1,0.1,0.1,0.15]),
        "plan": rng.choice(["free","pro","enterprise"], size=n_users, p=[0.6,0.3,0.1]),
        "account_age_days": rng.integers(1, 2500, size=n_users),
    })

    # IP reputation table
    n_ips = 5000
    ip_rep = pd.DataFrame({
        "ip": [f"10.{rng.integers(0,256)}.{rng.integers(0,256)}.{rng.integers(0,256)}" for _ in range(n_ips)],
        "ip_risk": np.clip(rng.normal(0.25, 0.2, size=n_ips), 0, 1),
        "is_known_bad": (rng.random(n_ips) < 0.03).astype(int)
    })

    start = datetime(2025, 1, 1)
    ts = [start + timedelta(minutes=int(x)) for x in rng.integers(0, 60*24*60, size=n_events)]  # ~60 days

    events = pd.DataFrame({
        "event_id": np.arange(1, n_events + 1),
        "user_id": rng.integers(1, n_users + 1, size=n_events),
        "timestamp": ts,
        "device_type": rng.choice(["mobile","desktop","tablet"], size=n_events, p=[0.5,0.4,0.1]),
        "browser": rng.choice(["chrome","firefox","safari","edge"], size=n_events, p=[0.55,0.15,0.2,0.1]),
        "is_vpn": (rng.random(n_events) < 0.2).astype(int),
        "failed_logins": rng.poisson(1.2, size=n_events),
        "session_duration": rng.exponential(120, size=n_events),
        "bytes_sent": rng.lognormal(10, 1.0, size=n_events),
        "bytes_received": rng.lognormal(10.2, 1.1, size=n_events),
        "ip": rng.choice(ip_rep["ip"].values, size=n_events),
        "success": (rng.random(n_events) > 0.15).astype(int)
    })

    # create a risk label with signal
    # join for signal generation (not the pipeline join; just generation)
    tmp = events.merge(users[["user_id","account_age_days","plan"]], on="user_id", how="left")
    tmp = tmp.merge(ip_rep[["ip","ip_risk","is_known_bad"]], on="ip", how="left")

    prob = (
        0.25*tmp["ip_risk"] +
        0.25*(tmp["failed_logins"] > 2).astype(int) +
        0.15*tmp["is_vpn"] +
        0.15*(tmp["account_age_days"] < 30).astype(int) +
        0.10*(tmp["plan"] == "free").astype(int) +
        0.20*tmp["is_known_bad"]
    )
    events["label"] = ((prob + rng.normal(0, 0.08, size=n_events)) > 0.55).astype(int)

    # inject missingness
    for col in ["session_duration","country","bytes_sent"]:
        idx = rng.choice(events.index, size=int(0.02*n_events), replace=False)
        events.loc[idx, col] = np.nan

    users.to_csv(RAW_DIR / "user_profiles.csv", index=False)
    ip_rep.to_csv(RAW_DIR / "ip_reputation.csv", index=False)
    events.to_csv(RAW_DIR / "auth_events.csv", index=False)

    logger.info(f"Wrote raw files to {RAW_DIR}")

if __name__ == "__main__":
    main()
