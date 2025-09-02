import os, duckdb

ROOT = os.getenv("DATA_ROOT", ".")
os.makedirs(os.path.join(ROOT, "artifacts"), exist_ok=True)
DB = os.path.join(ROOT, "artifacts/coffee.duckdb")

con = duckdb.connect(DB)

# 1) Raw orders from bronze
con.execute("""
CREATE OR REPLACE TABLE orders AS
SELECT * FROM read_parquet($p, hive_partitioning=0);
""", {'p': os.path.join(ROOT, "data/bronze/orders/*.parquet")})

# 2) Minute metrics: prefer streaming silver; if absent, derive from bronze
silver_glob = os.path.join(ROOT, "data/silver/minute_metrics/*.parquet")
try:
    con.execute("""
    CREATE OR REPLACE TABLE minute_metrics AS
    SELECT * FROM read_parquet($g, hive_partitioning=0);
    """, {'g': silver_glob})
except Exception:
    # Fallback: compute minute revenue in DuckDB directly from bronze
    con.execute("""
    CREATE OR REPLACE TABLE minute_metrics AS
    WITH t AS (
      SELECT
        date_trunc('minute', event_ts) AS minute_start,
        date_trunc('minute', event_ts) + INTERVAL 1 minute AS minute_end,
        total_price
      FROM orders
    )
    SELECT minute_start, minute_end, SUM(total_price) AS revenue
    FROM t
    GROUP BY 1,2
    ORDER BY 1;
    """)

# 3) Daily KPIs (gold)
con.execute("""
CREATE OR REPLACE TABLE daily_kpis AS
SELECT * FROM read_csv_auto($g, header=true);
""", {'g': os.path.join(ROOT, "data/gold/daily_key_metrics_*/part-*.csv")})


def _sql_quote(path: str) -> str:
    # escape single quotes for safe SQL string literal
    return path.replace("'", "''")

out_daily = os.path.join(ROOT, "artifacts", "daily_kpis.csv")
con.execute(
    f"""
    COPY (SELECT * FROM daily_kpis ORDER BY date)
    TO '{_sql_quote(out_daily)}' WITH (HEADER, DELIMITER ',');
    """
)

out_minutes_today = os.path.join(ROOT, "artifacts", "minute_metrics_2025-09-02.csv")
con.execute(
    f"""
    COPY (
      SELECT *
      FROM minute_metrics
      WHERE CAST(minute_start AS DATE) = DATE '2025-09-02'
      ORDER BY minute_start
    )
    TO '{_sql_quote(out_minutes_today)}' WITH (HEADER, DELIMITER ',');
    """
)

print("Wrote:", DB)
print("CSV:", out_daily)
print("CSV:", out_minutes_today)
print("Sample daily_kpis:")
print(con.sql("SELECT * FROM daily_kpis ORDER BY date LIMIT 10").df())

