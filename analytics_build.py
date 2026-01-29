import duckdb
from src.config import DB_PATH
from src.utils_logging import get_logger

logger = get_logger()

def build_models() -> None:
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

    # dim_date
    con.execute("DROP TABLE IF EXISTS analytics.dim_date;")
    con.execute("""
        CREATE TABLE analytics.dim_date AS
        SELECT DISTINCT
            CAST(timestamp AS DATE) AS date,
            EXTRACT(year FROM timestamp) AS year,
            EXTRACT(month FROM timestamp) AS month,
            EXTRACT(day FROM timestamp) AS day,
            EXTRACT(dow FROM timestamp) AS day_of_week
        FROM staging.stg_events;
    """)

    # dim_user
    con.execute("DROP TABLE IF EXISTS analytics.dim_user;")
    con.execute("""
        CREATE TABLE analytics.dim_user AS
        SELECT
            user_id,
            country,
            plan,
            account_age_days
        FROM staging.stg_users;
    """)

    # fact_auth_event
    con.execute("DROP TABLE IF EXISTS analytics.fact_auth_event;")
    con.execute("""
        CREATE TABLE analytics.fact_auth_event AS
        SELECT
            e.event_id,
            e.user_id,
            CAST(e.timestamp AS DATE) AS date,
            e.timestamp,
            e.device_type,
            e.browser,
            e.is_vpn,
            e.failed_logins,
            e.session_duration,
            e.bytes_sent,
            e.bytes_received,
            e.ip,
            r.ip_risk,
            r.is_known_bad,
            e.success,
            e.label
        FROM staging.stg_events e
        LEFT JOIN staging.stg_ip_rep r USING (ip);
    """)

    # mart: daily KPIs
    con.execute("DROP TABLE IF EXISTS analytics.mart_daily_kpis;")
    con.execute("""
        CREATE TABLE analytics.mart_daily_kpis AS
        SELECT
            date,
            COUNT(*) AS total_events,
            AVG(success) AS success_rate,
            AVG(label) AS suspicious_rate,
            AVG(ip_risk) AS avg_ip_risk,
            AVG(failed_logins) AS avg_failed_logins,
            SUM(CASE WHEN is_known_bad=1 THEN 1 ELSE 0 END) AS known_bad_events
        FROM analytics.fact_auth_event
        GROUP BY 1
        ORDER BY 1;
    """)

    # mart: risk segments
    con.execute("DROP TABLE IF EXISTS analytics.mart_risk_segments;")
    con.execute("""
        CREATE TABLE analytics.mart_risk_segments AS
        SELECT
            CASE
                WHEN ip_risk >= 0.7 OR is_known_bad=1 THEN 'high'
                WHEN ip_risk >= 0.4 THEN 'medium'
                ELSE 'low'
            END AS risk_segment,
            COUNT(*) AS events,
            AVG(label) AS suspicious_rate,
            AVG(success) AS success_rate
        FROM analytics.fact_auth_event
        GROUP BY 1
        ORDER BY suspicious_rate DESC;
    """)

    con.close()
    logger.info("Built analytics models: dims, fact, marts")

if __name__ == "__main__":
    build_models()