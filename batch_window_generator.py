CREATE OR REPLACE PROCEDURE MANAGE_HOURLY_TIME_WINDOWS(
    PIPELINE_NAME VARCHAR,
    INDEX_NAME VARCHAR,
    QUERY_WINDOW_DELTA FLOAT,
    TZ VARCHAR,
    DATABASE_NAME VARCHAR,
    SCHEMA_NAME VARCHAR,
    TABLE_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark.functions as F
from datetime import datetime, timedelta, time

def main(session, pipeline_name, index_name, query_window_delta, tz, database_name, schema_name, table_name):
    """
    Identifies and inserts missing hourly records into the AUDIT table.
    - Sets 'failed' status for backfilled records.
    - Provides NULL for columns without default values.
    """
    
    fully_qualified_table_name = [database_name, schema_name, table_name]

    # --- 1. Identify the Latest Timestamp ---
    latest_timestamp_df = session.table(fully_qualified_table_name) \
        .filter((F.col("PIPELINE_NAME") == pipeline_name) & (F.col("INDEX_NAME") == index_name)) \
        .select(F.max("QUERY_WINDOW_END_TS").alias("LATEST_TS"))

    latest_timestamp_result = latest_timestamp_df.collect()
    latest_timestamp = latest_timestamp_result[0]['LATEST_TS']

    if latest_timestamp is None:
        now_in_tz = datetime.now(datetime.timezone(timedelta(hours=int(tz.split(':')[0]))))
        yesterday_midnight = datetime.combine(now_in_tz.date() - timedelta(days=1), time(0), tzinfo=now_in_tz.tzinfo)
        start_ts = yesterday_midnight - timedelta(days=1)
    else:
        start_ts = latest_timestamp

    # --- 2. Calculate Target Timestamp (Yesterday's Midnight) ---
    hours_offset = int(tz.split(':')[0])
    minutes_offset = int(tz.split(':')[1]) if ':' in tz else 0
    timezone_info = datetime.timezone(timedelta(hours=hours_offset, minutes=minutes_offset))
    now_in_tz = datetime.now(timezone_info)
    target_midnight = datetime.combine(now_in_tz.date() - timedelta(days=1), time(0), tzinfo=timezone_info)

    # --- 3. Determine and 4. Insert Missing Hourly Windows ---
    if start_ts >= target_midnight:
        return f"No missing records to insert. The latest record is at or beyond yesterday's midnight."

    records_to_insert = []
    current_ts = start_ts

    while current_ts < target_midnight:
        next_ts = current_ts + timedelta(minutes=query_window_delta)
        if next_ts > target_midnight:
            break

        # Create a complete record for insertion
        # Sets STATUS to 'failed' and other nullable columns to None (which becomes NULL)
        records_to_insert.append({
            "PIPELINE_NAME": pipeline_name,
            "INDEX_NAME": index_name,
            "QUERY_WINDOW_START_TS": current_ts,
            "QUERY_WINDOW_END_TS": next_ts,
            "QUERY_WINDOW_DELTA": query_window_delta,
            "STATUS": 'failed',
            # Explicitly setting NULL for columns without a default value
            "DAG_RUN_TS": None,
            "ELASTICSEARCH_COUNT": None,
            "SNOWFLAKE_COUNT": None,
            "COUNT_MATCHED": None,
            "DIFF_COUNT": None,
            "DIFF_PERCENTAGE": None,
            "ELT_START_TS": None,
            "AUDIT_START_TS": None
        })
        current_ts = next_ts

    if records_to_insert:
        # The schema list must now include all the columns we are inserting
        schema = [
            "PIPELINE_NAME", "INDEX_NAME", "QUERY_WINDOW_START_TS", "QUERY_WINDOW_END_TS", 
            "QUERY_WINDOW_DELTA", "STATUS", "DAG_RUN_TS", "ELASTICSEARCH_COUNT", 
            "SNOWFLAKE_COUNT", "COUNT_MATCHED", "DIFF_COUNT", "DIFF_PERCENTAGE", 
            "ELT_START_TS", "AUDIT_START_TS"
        ]
        
        missing_windows_df = session.create_dataframe(records_to_insert, schema=schema)

        # Append the new, complete records to your AUDIT table
        missing_windows_df.write.mode("append").save_as_table(fully_qualified_table_name)
        
        return f"Successfully inserted {len(records_to_insert)} missing records into {'.'.join(fully_qualified_table_name)} with status 'failed'."
    else:
        return "No missing records to insert."
$$;
