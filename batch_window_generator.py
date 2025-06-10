CREATE OR REPLACE PROCEDURE insert_missing_windows(
    pipeline_namer STRING,
    index_name STRING,
    query_window_delta INT,
    tz STRING
)
RETURNS STRING
LANGUAGE PYTHON
EXECUTE AS CALLER
AS
$$
import datetime

# Helper: get yesterday's midnight in the right timezone
def get_yesterday_midnight(tz):
    session = snowflake.snowpark.Session.builder.getOrCreate()
    result = session.sql(f"SELECT TO_TIMESTAMP_TZ(TO_VARCHAR(DATEADD(day, -1, CURRENT_DATE())) || ' 00:00:00 {tz}', 'YYYY-MM-DD HH24:MI:SS TZHTZM')").collect()
    return result[0][0]

def run(session, pipeline_namer, index_name, query_window_delta, tz):
    # 1. Get latest query_windows_end_ts
    latest = session.sql(f"""
        SELECT MAX(query_windows_end_ts) 
        FROM YOUR_SCHEMA.YOUR_TABLE
        WHERE pipeline_namer = %s AND index_name = %s
    """, (pipeline_namer, index_name)).collect()[0][0]
    
    if not latest:
        return 'No latest record found. Aborting.'
    
    # 2. Get yesterday's midnight with timezone
    yesterday_midnight = get_yesterday_midnight(tz)
    
    # 3. Build list of new windows
    window_start = latest
    window_delta = datetime.timedelta(minutes=int(query_window_delta))
    rows_to_insert = []
    
    while True:
        window_end = window_start + window_delta
        if window_end > yesterday_midnight:
            break
        rows_to_insert.append((pipeline_namer, index_name, window_start, window_end, query_window_delta))
        window_start = window_end

    # 4. Insert rows if any
    if rows_to_insert:
        session.write_pandas(
            pandas.DataFrame(rows_to_insert, columns=[
                "pipeline_namer", "index_name", "query_windows_start_ts", "query_windows_end_ts", "query_window_delta"
            ]),
            "YOUR_TABLE",
            schema="YOUR_SCHEMA",
            overwrite=False
        )
        return f"Inserted {len(rows_to_insert)} windows."
    else:
        return "No windows to insert."

$$;
