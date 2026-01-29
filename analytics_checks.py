import duckdb
from src.config import DB_PATH
from src.utils_logging import get_logger

logger = get_logger()

def run_checks() -> None:
    con = duckdb.connect(str(DB_PATH))

    # Not null checks
    nn = con.execute("""
        SELECT
          SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_id,
          SUM(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END) AS null_event_id
        FROM analytics.fact_auth_event;
    """).fetchone()

    if nn[0] > 0 or nn[1] > 0:
        raise ValueError(f"NOT NULL failed: {nn}")

    # Unique key checks
    dupes = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT event_id, COUNT(*) c
            FROM analytics.fact_auth_event
            GROUP BY 1
            HAVING c > 1
        );
    """).fetchone()[0]
    if dupes > 0:
        raise ValueError(f"UNIQUE failed: duplicate event_id groups={dupes}")

    # Accepted values checks
    bad_device = con.execute("""
        SELECT COUNT(*) FROM analytics.fact_auth_event
        WHERE device_type NOT IN ('mobile','desktop','tablet');
    """).fetchone()[0]
    if bad_device > 0:
        raise ValueError(f"Accepted values failed for device_type: {bad_device}")

    con.close()
    logger.info("Analytics checks passed ✅")

if __name__ == "__main__":
    run_checks()