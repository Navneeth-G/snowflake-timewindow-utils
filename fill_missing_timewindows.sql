WITH latest_window AS (
  SELECT
    MAX(query_windows_end_ts) AS latest_end
  FROM your_table
  WHERE pipeline_namer = 'abc'
    AND index_name = 'a1'
),
yesterday_midnight AS (
  -- Make sure to use the right timezone offset (example: '-07:00')
  SELECT 
    -- Use your actual timezone offset here
    TO_TIMESTAMP_TZ(
      TO_VARCHAR(DATEADD(day, -1, CURRENT_DATE())) || ' 00:00:00 -07:00', 
      'YYYY-MM-DD HH24:MI:SS TZHTZM'
    ) AS midnight
),
generated_windows AS (
  SELECT
    (SELECT latest_end FROM latest_window) AS window_start,
    DATEADD(minute, 60, (SELECT latest_end FROM latest_window)) AS window_end
  UNION ALL
  SELECT
    window_end AS window_start,
    DATEADD(minute, 60, window_end) AS window_end
  FROM generated_windows
  WHERE window_end < (SELECT midnight FROM yesterday_midnight)
)
INSERT INTO your_table (pipeline_namer, index_name, query_windows_start_ts, query_windows_end_ts, query_window_delta)
SELECT
  'abc',
  'a1',
  window_start,
  window_end,
  60
FROM generated_windows
WHERE window_end <= (SELECT midnight FROM yesterday_midnight);
