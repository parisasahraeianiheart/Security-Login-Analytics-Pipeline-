import duckdb
from src.config import DB_PATH, STAGING_DIR
from src.utils_logging import get_logger

logger = get_logger()

def load_to_duckdb() -> None:
    con = duckdb.connect(str(DB_PATH))

    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    con.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

    # idempotent loads: replace tables
    con.execute("DROP TABLE IF EXISTS staging.stg_users;")
    con.execute("DROP TABLE IF EXISTS staging.stg_ip_rep;")
    con.execute("DROP TABLE IF EXISTS staging.stg_events;")

    con.execute(f"""
        CREATE TABLE staging.stg_users AS
        SELECT * FROM read_parquet('{STAGING_DIR / "stg_users.parquet"}');
    """)
    con.execute(f"""
        CREATE TABLE staging.stg_ip_rep AS
        SELECT * FROM read_parquet('{STAGING_DIR / "stg_ip_rep.parquet"}');
    """)
    con.execute(f"""
        CREATE TABLE staging.stg_events AS
        SELECT * FROM read_parquet('{STAGING_DIR / "stg_events.parquet"}');
    """)

    con.close()
    logger.info(f"Loaded staging tables into {DB_PATH}")

if __name__ == "__main__":
    load_to_duckdb()