# snowflake-timewindow-utils
Utilities for generating, filling, and managing time-windowed records in Snowflake tables. Includes example stored procedures (in Python) for inserting missing time windows, automating ETL batch window creation, and teaching best practices for time series data handling in Snowflake.


---

## **Automated Time Window Insertion in Snowflake**

### **Background**

In time-series or batch data processing, it’s common to partition data into fixed windows (e.g., 1-hour windows) for ETL pipelines, monitoring, or analytics. Ensuring every expected time window exists is critical for accurate downstream processing and for avoiding data loss or duplicated analysis.

### **Requirement**

We have a Snowflake table that stores time window information for a pipeline, with the following relevant columns:

* `pipeline_namer` (string): Identifies the pipeline.
* `index_name` (string): Identifies the index or sub-pipeline.
* `query_windows_start_ts` (TIMESTAMP\_TZ): Window start time, with timezone.
* `query_windows_end_ts` (TIMESTAMP\_TZ): Window end time, with timezone.
* `query_window_delta` (integer): Window duration in minutes (typically 60 for 1 hour).

**Goal:**
Whenever the ETL process is triggered, automatically insert all 1-hour window records that are missing between the last known window and up to the previous day’s midnight (00:00:00), exclusive.

* `pipeline_namer`, `index_name`, and `query_window_delta` are constants for each run.
* Only insert windows that are not already present, starting right after the latest existing window.
* All timestamp calculations must respect the correct timezone.

### **Practical Example**

Suppose your last record is:

* `pipeline_namer`: `abc`
* `index_name`: `a1`
* `query_windows_end_ts`: `2025-05-11 10:00:00.000 -0700`

If today is `2025-05-13`, you want to generate and insert 1-hour windows for:

* 2025-05-11 10:00:00 -0700 → 2025-05-11 11:00:00 -0700
* 2025-05-11 11:00:00 -0700 → 2025-05-11 12:00:00 -0700
  ...
  All the way **up to, but not including** `2025-05-12 00:00:00 -0700` (previous day’s midnight).

### **Why This Is Needed**

* Maintains completeness in your time windowed data.
* Enables downstream jobs or reports to reliably query all expected time windows.
* Prevents data gaps or errors in batch processing.

### **Automated Approach**

* **Step 1:** Find the latest `query_windows_end_ts` for the given pipeline/index.
* **Step 2:** Calculate yesterday’s midnight (00:00:00) in the correct timezone.
* **Step 3:** Generate all missing 1-hour windows in this range.
* **Step 4:** Bulk insert the missing records into the Snowflake table.

### **Key Considerations**

* All timestamps use `TIMESTAMP_TZ` to ensure time zone awareness and correctness.
* No windows are created for today or in the future—only up to yesterday’s midnight.
* The process is idempotent and does not create duplicate records.

---

### **Summary**

This process ensures your time window tracking table in Snowflake is always up to date, with zero gaps, for every pipeline and index combination. It is generic, reusable, and easy to automate—making it ideal for ETL pipelines and data engineering education.


