CREATE OR REPLACE PROCEDURE MANAGE_HOURLY_TIME_WINDOWS_V2(
    PIPELINE_NAME VARCHAR,
    INDEX_NAME VARCHAR,
    QUERY_WINDOW_DELTA FLOAT,
    TZ VARCHAR,
    DATABASE_NAME VARCHAR,
    SCHEMA_NAME VARCHAR,
    TABLE_NAME VARCHAR,
    DRY_RUN BOOLEAN
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark.functions as F
from datetime import datetime, timedelta, time, timezone
import json

def main(session, pipeline_name, index_name, query_window_delta, tz, database_name, schema_name, table_name, dry_run):
    """
    This version generates the timestamp within the Python script itself.
    NOTE: Using the database's DEFAULT by setting the value to None is the recommended best practice.
    """
    
    fully_qualified_table_name = [database_name, schema_name, table_name]

    # --- Logic to find the latest timestamp (Same as before) ---
    latest_timestamp_df = session.table(fully_qualified_table_name).filter((F.col("PIPELINE_NAME") == pipeline_name) & (F.col("INDEX_NAME") == index_name)).select(F.max("QUERY_WINDOW_END_TS").alias("LATEST_TS"))
    latest_timestamp_result = latest_timestamp_df.collect()
    latest_timestamp = latest_timestamp_result[0]['LATEST_TS']
    hours_offset_calc = int(tz.split(':')[0])
    now_in_tz_calc = datetime.now(timezone(timedelta(hours=hours_offset_calc)))
    if latest_timestamp is None:
        yesterday_midnight_calc = datetime.combine(now_in_tz_calc.date() - timedelta(days=1), time(0), tzinfo=now_in_tz_calc.tzinfo)
        start_ts = yesterday_midnight_calc - timedelta(days=1)
    else:
        start_ts = latest_timestamp
    hours_offset = int(tz.split(':')[0])
    minutes_offset = int(tz.split(':')[1]) if ':' in tz else 0
    timezone_info = timezone(timedelta(hours=hours_offset, minutes=minutes_offset))
    now_in_tz = datetime.now(timezone_info)
    target_midnight = datetime.combine(now_in_tz.date() - timedelta(days=1), time(0), tzinfo=timezone_info)
    if start_ts >= target_midnight: return "No missing records to insert."

    # --- Generate Records ---
    records_to_insert = []
    current_ts = start_ts

    # ** KEY CHANGE IS HERE **
    # Get the current time once for all records in this batch.
    # We use timezone.utc for a standardized, timezone-aware timestamp.
    python_generated_timestamp = datetime.now(timezone.utc)

    while current_ts < target_midnight:
        next_ts = current_ts + timedelta(minutes=query_window_delta)
        if next_ts > target_midnight: break
        records_to_insert.append({
            "DAG_RUN_TS": None, "PIPELINE_NAME": pipeline_name, "INDEX_NAME": index_name,
            "QUERY_WINDOW_START_TS": current_ts, "QUERY_WINDOW_END_TS": next_ts,
            "QUERY_WINDOW_DELTA": query_window_delta, "ELASTICSEARCH_COUNT": None,
            "SNOWFLAKE_COUNT": None, "COUNT_MATCHED": None, "DIFF_COUNT": None,
            "DIFF_PERCENTAGE": None, "ELT_START_TS": None, "AUDIT_START_TS": None,
            # Using the Python-generated timestamp here
            "REC_INSERTED_TS": python_generated_timestamp,
            "REC_LAST_UPDATED_TS": python_generated_timestamp,
            "STATUS": 'failed', "RETRY_ATTEMPT": 0
        })
        current_ts = next_ts
    
    if not records_to_insert: return "No missing records to insert."

    # --- Dry Run or Actual Insert (Same as before) ---
    if dry_run:
        preview_json = json.dumps(records_to_insert, indent=4, default=str)
        return f"--- DRY RUN PREVIEW ---\nTotal records to be inserted: {len(records_to_insert)}\n\n{preview_json}"
    else:
        schema = [
            "DAG_RUN_TS", "PIPELINE_NAME", "INDEX_NAME", "QUERY_WINDOW_START_TS",
            "QUERY_WINDOW_END_TS", "QUERY_WINDOW_DELTA", "ELASTICSEARCH_COUNT",
            "SNOWFLAKE_COUNT", "COUNT_MATCHED", "DIFF_COUNT", "DIFF_PERCENTAGE",
            "ELT_START_TS", "AUDIT_START_TS", "REC_INSERTED_TS",
            "REC_LAST_UPDATED_TS", "STATUS", "RETRY_ATTEMPT"
        ]
        missing_windows_df = session.create_dataframe(records_to_insert, schema=schema)
        missing_windows_df.write.mode("append").save_as_table(fully_qualified_table_name)
        return f"--- INSERTION COMPLETE ---\nSuccessfully inserted {len(records_to_insert)} missing records into {'.'.join(fully_qualified_table_name)}."
$$;
