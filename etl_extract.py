import pandas as pd
from src.config import RAW_DIR
from src.utils_quality import assert_row_count
from src.utils_logging import get_logger

logger = get_logger()

def extract() -> dict[str, pd.DataFrame]:
    users = pd.read_csv(RAW_DIR / "user_profiles.csv")
    ip_rep = pd.read_csv(RAW_DIR / "ip_reputation.csv")
    events = pd.read_csv(RAW_DIR / "auth_events.csv")

    assert_row_count(users, 100)
    assert_row_count(ip_rep, 100)
    assert_row_count(events, 1000)

    logger.info("Extracted raw datasets")
    return {"users": users, "ip_rep": ip_rep, "events": events}

if __name__ == "__main__":
    extract()
