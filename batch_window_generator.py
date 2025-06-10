CREATE OR REPLACE PROCEDURE ensure_complete_time_windows(
    PIPELINE_NAMER VARCHAR,
    INDEX_NAME VARCHAR,
    QUERY_WINDOW_DELTA INT,
    TZ VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark.functions as F
from snowflake.snowpark.types import TimestampType
from datetime import datetime, timedelta, time

def main(session, pipeline_namer, index_name, query_window_delta, tz):
    """
    Identifies and inserts missing hourly records for a specific data pipeline index
    up to the previous day's midnight.

    Args:
        session: The Snowpark session object.
        pipeline_namer: The identifier for the data pipeline.
        index_name: The identifier for the index.
        query_window_delta: The time window delta in minutes (e.g., 60).
        tz: The timezone as a string (e.g., '-07:00').
    """

    # --- 1. Identify the Latest Timestamp ---
    # Get the most recent 'query_windows_end_ts' for the given pipeline and index.
    # If no record exists, use a default start date (e.g., a known past date).
    latest_timestamp_df = session.table("YOUR_TABLE_NAME") \
        .filter((F.col("PIPELINE_NAMER") == pipeline_namer) & (F.col("INDEX_NAME") == index_name)) \
        .select(F.max("QUERY_WINDOWS_END_TS").alias("LATEST_TS"))

    latest_timestamp_result = latest_timestamp_df.collect()
    latest_timestamp = latest_timestamp_result[0]['LATEST_TS']

    # If the table is empty for this index, start from a logical point, e.g., two days ago.
    # This avoids excessively long backfills on the first run.
    if latest_timestamp is None:
        # Default to starting from 48 hours before yesterday's midnight if no record exists
        now_in_tz = datetime.now(datetime.timezone(timedelta(hours=int(tz.split(':')[0]))))
        yesterday_midnight = datetime.combine(now_in_tz.date() - timedelta(days=1), time(0), tzinfo=now_in_tz.tzinfo)
        start_ts = yesterday_midnight - timedelta(days=1)
    else:
        start_ts = latest_timestamp

    # --- 2. Calculate Target Timestamp (Yesterday's Midnight) ---
    # Determine the timezone object from the string offset.
    hours_offset = int(tz.split(':')[0])
    minutes_offset = int(tz.split(':')[1]) if ':' in tz else 0
    timezone_info = datetime.timezone(timedelta(hours=hours_offset, minutes=minutes_offset))

    # Get the current time in the specified timezone to correctly identify "yesterday".
    now_in_tz = datetime.now(timezone_info)
    target_midnight = datetime.combine(now_in_tz.date() - timedelta(days=1), time(0), tzinfo=timezone_info)

    # --- 3. Determine and 4. Insert Missing Hourly Windows ---
    # Proceed only if the latest timestamp is before our target.
    if start_ts >= target_midnight:
        return f"No missing records to insert. The latest record is at or beyond yesterday's midnight."

    records_to_insert = []
    current_ts = start_ts

    # Generate hourly intervals until we reach yesterday's midnight.
    while current_ts < target_midnight:
        next_ts = current_ts + timedelta(minutes=query_window_delta)
        
        # Ensure we do not insert any records for today or the future.
        if next_ts > target_midnight:
            break

        # Prepare the record for insertion.
        records_to_insert.append({
            "PIPELINE_NAMER": pipeline_namer,
            "INDEX_NAME": index_name,
            "QUERY_WINDOWS_START_TS": current_ts,
            "QUERY_WINDOWS_END_TS": next_ts,
            "QUERY_WINDOW_DELTA": query_window_delta
        })
        current_ts = next_ts

    # If there are records to insert, create a DataFrame and write it to the table.
    if records_to_insert:
        # Define the schema for the new DataFrame.
        schema = ["PIPELINE_NAMER", "INDEX_NAME", "QUERY_WINDOWS_START_TS", "QUERY_WINDOWS_END_TS", "QUERY_WINDOW_DELTA"]
        
        # Create a Snowpark DataFrame from the list of dictionaries.
        missing_windows_df = session.create_dataframe(records_to_insert, schema=schema)

        # Append the new records to your target table.
        missing_windows_df.write.mode("append").save_as_table("YOUR_TABLE_NAME")
        
        return f"Successfully inserted {len(records_to_insert)} missing records."
    else:
        return "No missing records to insert."

$$;
