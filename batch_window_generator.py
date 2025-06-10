CREATE OR REPLACE PROCEDURE insert_missing_windows(
    pipeline_namer STRING,
    index_name STRING,
    query_window_delta INT,
    tz STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import datetime

def run(session, pipeline_namer, index_name, query_window_delta, tz):
    # 1. Get the latest query_windows_end_ts for this pipeline and index
    result = session.sql(f"""
        SELECT MAX(query_windows_end_ts) AS latest_end
        FROM YOUR_SCHEMA.YOUR_TABLE
        WHERE pipeline_namer = '{pipeline_namer}'
          AND index_name = '{index_name}'
    """).collect()
    latest_end = result[0]['LATEST_END']
    if latest_end is None:
        return 'No existing window found. Aborting.'

    # 2. Get yesterday's midnight in the right timezone (returns TIMESTAMP_TZ)
    result = session.sql(f"""
        SELECT TO_TIMESTAMP_TZ(
            TO_VARCHAR(DATEADD(day, -1, CURRENT_DATE())) || ' 00:00:00 {tz}',
            'YYYY-MM-DD HH24:MI:SS TZHTZM'
        ) AS midnight
    """).collect()
    midnight = result[0]['MIDNIGHT']

    # 3. Prepare new windows to insert
    window_start = latest_end
    window_delta = datetime.timedelta(minutes=int(query_window_delta))
    rows_to_insert = []

    while True:
        window_end = window_start + window_delta
        if window_end > midnight:
            break
        rows_to_insert.append((pipeline_namer, index_name, window_start, window_end, query_window_delta))
        window_start = window_end

    # 4. Insert missing windows
    if rows_to_insert:
        values_str = ",".join(
            [f"""('{row[0]}', '{row[1]}', TO_TIMESTAMP_TZ('{row[2]}'), TO_TIMESTAMP_TZ('{row[3]}'), {row[4]})""" for row in rows_to_insert]
        )
        insert_sql = f"""
            INSERT INTO YOUR_SCHEMA.YOUR_TABLE
            (pipeline_namer, index_name, query_windows_start_ts, query_windows_end_ts, query_window_delta)
            VALUES {values_str}
        """
        session.sql(insert_sql).collect()
        return f"Inserted {len(rows_to_insert)} new windows."
    else:
        return "No missing windows to insert."

$$;
