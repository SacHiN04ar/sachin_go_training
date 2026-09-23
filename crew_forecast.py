from datetime import datetimenow = datetime.datetime.now().strftime("%a, %d %b %Y %H:%M:%S")
current_time = datetime.datetime.now().strftime("%H:%M:%S.%f")[:9]

tmstp = ""
title = ""

CNT = sqlobs
obs = sqlobs
#TODO: Libnames  to be modified by Infogain
#LIBNAME CAM '/lustrefs/fea/warehouse/fltmart/data/FltOps/Data/FLTMARTData/appdata/cam';

#LIBNAME RESULTS '/lustrefs/fea/warehouse/fltmart/data/FltOps/Data/FLTMARTData/flight/CAM_results';

#LIBNAME PBI_LOAD '/lustrefs/fea/warehouse/fltmart/data/FltOps/Data/pbi_load';

#LIBNAME FO_SRC '/lustrefs/fea/warehouse/fltmart/data/FltOps/Data/SourceData';
#BEGIN CHUNK 1#
# This chunk initializes timestamp and observation count variables and sets up library paths for data access.

def initialize_globals(sqlobs):
    """
    Sets global variables for timestamp and observation counts, and establishes library paths as Unity Catalog schema mappings.
    Args:
        sqlobs (int): Number of SQL observations, used for CNT and obs variables.
    Returns:
        dict: Dictionary containing now, tmstp, title, CNT, obs, and libname mappings.
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Format mimicking dtwkdatx20.
    tmstp = now
    title = None  # Placeholder, SAS macro variable
    CNT = sqlobs
    obs = sqlobs

    libnames = {
        'CAM': 'catalog.schema.cam',
        'RESULTS': 'catalog.schema.cam_results',
        'PBI_LOAD': 'catalog.schema.pbi_load',
        'FO_SRC': 'catalog.schema.source_data'
    }

    return {
        'now': now,
        'tmstp': tmstp,
        'title': title,
        'CNT': CNT,
        'obs': obs,
        'libnames': libnames
    }

# NOTE: LIBNAME statements in SAS are mapped to Unity Catalog schema references in Databricks.
# These can be used in SQL as catalog.schema.table.

#END OF CHUNK 1#
#BEGIN CHUNK 2#
# The following code logs the opening of the log file and creates a new table WORK.SCENARIOS
# containing TITLE, TIMESTAMP, and a generated row number (ROW) for records where PUBLISHED = 'Y'.
# The OPTIONS statement is not converted, as it contains SAS-specific session options.


now = datetime.now().strftime("%a, %d %b %Y %H:%M:%S")
print(f"NOTE: Log file opened at {now}")

# Create or replace the WORK.SCENARIOS table as per PROC SQL logic
spark.sql("""
CREATE OR REPLACE TABLE WORK.SCENARIOS AS
SELECT
  TITLE,
  TIMESTAMP,
  ROW_NUMBER() OVER (ORDER BY MONOTONIC()) AS ROW
FROM
  CAM.CAM_RESULTS_MASTER
WHERE
  PUBLISHED = 'Y'
""")
#END OF CHUNK 2#
#BEGIN CHUNK 3#
# BEGIN SUBCHUNK 1
def combine_scenarios(cnt):
    """
    This function emulates the SAS 'combine_scenarios' macro logic for Databricks.
    It checks if the scenario count ('cnt') is greater than 0, and, if so, initiates a loop.
    The loop is currently set for a single iteration (from 1 to 1), matching the SAS code's structure.
    You can extend the loop to 'cnt' as needed for full scenario processing in downstream steps.
    """
    if cnt > 0:
        for i in range(1, 2):  # Equivalent to SAS: %do i=1 %to 1;
            pass  # Placeholder for downstream logic per iteration
# END OF SUBCHUNK 1
# BEGIN SUBCHUNK 2
# SUBCHUNK 2: Retrieve timestamp and title from WORK.SCENARIOS for the current scenario row.
tmstp_row = spark.sql(f"""SELECT timestamp FROM WORK.SCENARIOS WHERE row = {i}""").collect()
tmstp = tmstp_row[0]['timestamp'] if tmstp_row else None

title_row = spark.sql(f"""SELECT title FROM WORK.SCENARIOS WHERE row = {i}""").collect()
title = title_row[0]['title'] if title_row else None
# END OF SUBCHUNK 2
# BEGIN SUBCHUNK 3
# This subchunk builds scenario-scoped working views from CAM tables, derives POSN/ACFT/SEAT/DOM, enumerates months and positions, creates a cross-join scaffold NEW_LINES1 with initialized metrics, and captures min/max month to Python variables for later steps. Requires Python variables: tmstp (timestamp literal) and title (string).

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW `WORK.CAM_RESULTS` AS
WITH base AS (
  SELECT
    UNIQUE_ID AS ID,
    EMP_NBR,
    EMP_NM,
    SENIORITY_NBR,
    FLY_POSITION,
    CASE WHEN FLY_POSITION <> 'N/A' THEN FLY_POSITION ELSE CUR_POS_1 END AS POSN,
    AGE_65_DT,
    PRIM_CLASS_CD,
    SEC_CLASS_CD,
    AWARD_TYPE_CD,
    DEF_PRIM_CLASS_CD,
    DEF_SEC_CLASS_CD,
    DEF_AWARD_TYPE_CD,
    DEF_POS,
    DIRECTION,
    DEFERRED_BIDMONTH,
    RETIRE_DT,
    RETIRE_MONTH,
    TRAIN_POS_1,
    CUR_POS_1,
    TRANS_UPG_CD_1,
    START_DT_1,
    VIPS_END_DT_1,
    TRAIN_START_MONTH_1,
    END_DT_1,
    TRAIN_END_MONTH_1,
    TRAIN_SOURCE_1,
    TRAIN_POS_2,
    CUR_POS_2,
    TRANS_UPG_CD_2,
    START_DT_2,
    VIPS_END_DT_2,
    TRAIN_START_MONTH_2,
    END_DT_2,
    TRAIN_END_MONTH_2,
    TRAIN_SOURCE_2,
    UNIQUE_ID
  FROM CAM.CAM_RESULTS_DETAILS
  WHERE TIMESTAMP = '{tmstp}'
)
SELECT
  ID,
  EMP_NBR,
  EMP_NM,
  SENIORITY_NBR,
  FLY_POSITION,
  POSN,
  AGE_65_DT,
  PRIM_CLASS_CD,
  SEC_CLASS_CD,
  AWARD_TYPE_CD,
  DEF_PRIM_CLASS_CD,
  DEF_SEC_CLASS_CD,
  DEF_AWARD_TYPE_CD,
  DEF_POS,
  DIRECTION,
  DEFERRED_BIDMONTH,
  RETIRE_DT,
  RETIRE_MONTH,
  TRAIN_POS_1,
  CUR_POS_1,
  TRANS_UPG_CD_1,
  START_DT_1,
  VIPS_END_DT_1,
  TRAIN_START_MONTH_1,
  END_DT_1,
  TRAIN_END_MONTH_1,
  TRAIN_SOURCE_1,
  TRAIN_POS_2,
  CUR_POS_2,
  TRANS_UPG_CD_2,
  START_DT_2,
  VIPS_END_DT_2,
  TRAIN_START_MONTH_2,
  END_DT_2,
  TRAIN_END_MONTH_2,
  TRAIN_SOURCE_2,
  SUBSTR(POSN, 1, 2) AS ACFT,
  CASE WHEN SUBSTR(POSN, 3, 1) = 'C' THEN 'CAP' ELSE 'FO' END AS SEAT,
  CASE
    WHEN SUBSTR(POSN, 4, 1) = 'M' THEN 'MEM'
    WHEN SUBSTR(POSN, 4, 1) = 'I' THEN 'IND'
    WHEN SUBSTR(POSN, 4, 1) = 'E' THEN 'EUR'
    WHEN SUBSTR(POSN, 4, 1) = 'A' THEN 'ANC'
    WHEN SUBSTR(POSN, 4, 1) = 'H' THEN 'HKG'
    WHEN SUBSTR(POSN, 4, 1) = 'L' THEN 'LAX'
    WHEN SUBSTR(POSN, 4, 1) = 'O' THEN 'OAK'
    ELSE ' '
  END AS DOM
FROM base
ORDER BY UNIQUE_ID
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW `WORK.CAM_MONTHS` AS
SELECT
  UNIQUE_ID,
  MONTH,
  VALUE
FROM CAM.CAM_RESULTS_MONTHS
WHERE TIMESTAMP = '{tmstp}'
ORDER BY UNIQUE_ID, MONTH
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW `WORK.MONTHS` AS
SELECT DISTINCT
  MONTH
FROM `WORK.CAM_MONTHS`
ORDER BY MONTH
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW `WORK.POSN` AS
SELECT DISTINCT
  POSN,
  ACFT,
  SEAT,
  DOM
FROM `WORK.CAM_RESULTS`
WHERE POSN NOT IN ('10CM','27CM','27FM','27SM')
ORDER BY POSN
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW `WORK.NEW_LINES1` AS
SELECT
  CAST(a.MONTH AS DATE) AS MONTH,
  b.ACFT,
  b.SEAT,
  b.DOM,
  CAST(0 AS DOUBLE) AS AVAIL,
  CAST(0 AS DOUBLE) AS FLX_FLYING_LINE,
  CAST(0 AS DOUBLE) AS TOT_AVAIL,
  CAST(0 AS DOUBLE) AS RETIRE,
  CAST(0 AS DOUBLE) AS BASE_TRANSFER_IN,
  CAST(0 AS DOUBLE) AS BASE_TRANSFER_OUT,
  CAST(0 AS DOUBLE) AS BASE_TRANSFER,
  CAST(0 AS DOUBLE) AS TRANSFER_OUT,
  CAST(0 AS DOUBLE) AS TRANSFER_IN,
  CAST(0 AS DOUBLE) AS NH_ACTIVATED,
  CAST(0 AS DOUBLE) AS FLX_TO_LINE,
  CAST(0 AS DOUBLE) AS NET_CHANGE,
  b.POSN,
  '{title}' AS TYPE
FROM `WORK.MONTHS` a, `WORK.POSN` b
ORDER BY b.POSN, a.MONTH
""")

_min_row = spark.sql("SELECT MIN(MONTH) AS min_mth FROM `WORK.CAM_MONTHS`").collect()[0]
min_mth = _min_row["min_mth"]

_max_row = spark.sql("SELECT MAX(MONTH) AS max_mth FROM `WORK.CAM_MONTHS`").collect()[0]
max_mth = _max_row["max_mth"]
# END OF SUBCHUNK 3
# BEGIN SUBCHUNK 4
"""
Subchunk 4 builds retirement datasets. It creates RETIRE_INFO from CAM_RESULTS with POSN resolution and ACFT/SEAT/DOM parsing, filtered by retirement month range; aggregates to RETIRE; then joins to NEW_LINES1 to create NEW_REITRE with retirements applied. ORDER BY clauses are preserved in views.
"""

spark.sql(f"""
CREATE OR REPLACE VIEW work.RETIRE_INFO AS
WITH base AS (
  SELECT
    EMP_NBR,
    RETIRE_MONTH AS MONTH,
    CASE
      WHEN TRAIN_POS_1 IS NULL THEN POSN
      WHEN TRAIN_POS_1 IS NOT NULL AND START_DT_1 > RETIRE_DT THEN POSN
      WHEN TRAIN_POS_1 IS NOT NULL AND START_DT_1 <= RETIRE_DT AND RETIRE_DT <= END_DT_1 THEN POSN
      ELSE TRAIN_POS_1
    END AS POSN
  FROM work.CAM_RESULTS
  WHERE RETIRE_MONTH BETWEEN DATE '{min_mth}' AND DATE '{max_mth}'
)
SELECT
  EMP_NBR,
  MONTH,
  POSN,
  substring(POSN, 1, 2) AS ACFT,
  CASE WHEN substring(POSN, 3, 1) = 'C' THEN 'CAP' ELSE 'FO' END AS SEAT,
  CASE
    WHEN substring(POSN, 4, 1) = 'M' THEN 'MEM'
    WHEN substring(POSN, 4, 1) = 'I' THEN 'IND'
    WHEN substring(POSN, 4, 1) = 'E' THEN 'EUR'
    WHEN substring(POSN, 4, 1) = 'A' THEN 'ANC'
    WHEN substring(POSN, 4, 1) = 'H' THEN 'HKG'
    WHEN substring(POSN, 4, 1) = 'L' THEN 'LAX'
    WHEN substring(POSN, 4, 1) = 'O' THEN 'OAK'
    ELSE ' '
  END AS DOM,
  CAST(-1.0 AS DOUBLE) AS RET
FROM base
ORDER BY EMP_NBR
""")

spark.sql("""
CREATE OR REPLACE VIEW work.RETIRE AS
SELECT
  POSN,
  MONTH,
  ACFT,
  SEAT,
  DOM,
  SUM(RET) AS RETIRE
FROM work.RETIRE_INFO
GROUP BY POSN, MONTH, ACFT, SEAT, DOM
""")

spark.sql("""
CREATE OR REPLACE VIEW work.NEW_REITRE AS
SELECT DISTINCT
  CAST(a.MONTH AS DATE) AS MONTH,
  a.ACFT,
  a.SEAT,
  a.DOM,
  a.AVAIL,
  a.FLX_FLYING_LINE,
  a.TOT_AVAIL,
  b.RETIRE,
  a.BASE_TRANSFER_IN,
  a.BASE_TRANSFER_OUT,
  a.BASE_TRANSFER,
  a.TRANSFER_OUT,
  a.TRANSFER_IN,
  a.NH_ACTIVATED,
  a.FLX_TO_LINE,
  a.NET_CHANGE,
  a.POSN,
  a.TYPE
FROM work.NEW_LINES1 a
JOIN work.RETIRE b
  ON a.MONTH = b.MONTH
 AND a.POSN = b.POSN
ORDER BY POSN, MONTH
""")
# END OF SUBCHUNK 4
# BEGIN SUBCHUNK 5
# SUBCHUNK 5: Update NEW_LINES1 with NEW_REITRE by POSN and MONTH, creating NEW_LINES2
spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_LINES2 AS
SELECT
    COALESCE(r.POSN, n.POSN) AS POSN,
    COALESCE(r.MONTH, n.MONTH) AS MONTH,
    -- If NEW_REITRE has a matching POSN and MONTH, take its RETIRE, else NEW_LINES1
    COALESCE(r.RETIRE, n.RETIRE) AS RETIRE,
    -- All other columns from NEW_LINES1, overwritten where NEW_REITRE is present
    n.TYPE,
    n.SEAT,
    n.DOM,
    n.BASE_TRANSFER,
    n.BASE_TRANSFER_IN,
    n.BASE_TRANSFER_OUT,
    n.REG_TRANSFER,
    n.REG_TRANSFER_IN,
    n.REG_TRANSFER_OUT
FROM NEW_LINES1 n
LEFT JOIN NEW_REITRE r
  ON n.POSN = r.POSN AND n.MONTH = r.MONTH
UNION ALL
SELECT
    r.POSN,
    r.MONTH,
    r.RETIRE,
    r.TYPE,
    r.SEAT,
    r.DOM,
    r.BASE_TRANSFER,
    r.BASE_TRANSFER_IN,
    r.BASE_TRANSFER_OUT,
    r.REG_TRANSFER,
    r.REG_TRANSFER_IN,
    r.REG_TRANSFER_OUT
FROM NEW_REITRE r
LEFT JOIN NEW_LINES1 n
  ON r.POSN = n.POSN AND r.MONTH = n.MONTH
WHERE n.POSN IS NULL AND n.MONTH IS NULL
""")
# END OF SUBCHUNK 5
# BEGIN SUBCHUNK 6
# SUBCHUNK 6: Create BASE_XFER_INFO, BASE_XFER_IN, and NEW_BASE_XFER_IN tables/views for base transfer calculations.
# This chunk builds transfer scenario info, aggregates base transfers, and merges into scenario lines for analysis.

spark.sql("""
CREATE OR REPLACE TEMP VIEW BASE_XFER_INFO AS
SELECT
  EMP_NBR,
  TRAIN_START_MONTH_1,
  TRAIN_END_MONTH_1,
  ACFT,
  SEAT,
  DOM,
  POSN,
  TRAIN_POS_1,
  CUR_POS_1,
  -1 AS PBT,
  1 AS BT
FROM CAM_RESULTS
WHERE SUBSTR(TRAIN_POS_1, 1, 3) = SUBSTR(CUR_POS_1, 1, 3)
  AND SUBSTR(TRAIN_POS_1, 4, 1) <> SUBSTR(CUR_POS_1, 4, 1)
  AND POSN = CUR_POS_1
ORDER BY EMP_NBR
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW BASE_XFER_IN AS
SELECT DISTINCT
  TRAIN_END_MONTH_1 AS MONTH,
  TRAIN_POS_1 AS POSN,
  SUM(BT) AS BASE_TRANSFER_IN
FROM BASE_XFER_INFO
GROUP BY TRAIN_END_MONTH_1, TRAIN_POS_1
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_BASE_XFER_IN AS
SELECT DISTINCT
  a.MONTH,
  a.ACFT,
  a.SEAT,
  a.DOM,
  a.AVAIL,
  a.FLX_FLYING_LINE,
  a.TOT_AVAIL,
  a.RETIRE,
  b.BASE_TRANSFER_IN,
  a.BASE_TRANSFER_OUT,
  (b.BASE_TRANSFER_IN + a.BASE_TRANSFER_OUT) AS BASE_TRANSFER,
  a.TRANSFER_OUT,
  a.TRANSFER_IN,
  a.NH_ACTIVATED,
  a.FLX_TO_LINE,
  a.NET_CHANGE,
  a.POSN,
  a.TYPE
FROM NEW_LINES2 a
INNER JOIN BASE_XFER_IN b
  ON a.MONTH = b.MONTH AND a.POSN = b.POSN
ORDER BY a.POSN, a.MONTH
""")
# END OF SUBCHUNK 6
# BEGIN SUBCHUNK 7
# SUBCHUNK 7: Update NEW_LINES3 with rows from NEW_LINES2 and NEW_BASE_XFER_IN matched by POSN and MONTH.
spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_LINES3 AS
SELECT 
    COALESCE(b.POSN, a.POSN) AS POSN,
    COALESCE(b.MONTH, a.MONTH) AS MONTH,
    COALESCE(b.BASE_TRANSFER, a.BASE_TRANSFER) AS BASE_TRANSFER,
    COALESCE(b.BASE_TRANSFER_IN, a.BASE_TRANSFER_IN) AS BASE_TRANSFER_IN,
    COALESCE(b.BASE_TRANSFER_OUT, a.BASE_TRANSFER_OUT) AS BASE_TRANSFER_OUT,
    COALESCE(b.RETIREMENT, a.RETIREMENT) AS RETIREMENT,
    COALESCE(b.REG_TRANSFER, a.REG_TRANSFER) AS REG_TRANSFER,
    COALESCE(b.NEW_LINE, a.NEW_LINE) AS NEW_LINE,
    COALESCE(b.NEW_LINE_TYPE, a.NEW_LINE_TYPE) AS NEW_LINE_TYPE
FROM NEW_LINES2 a
FULL OUTER JOIN NEW_BASE_XFER_IN b
ON a.POSN = b.POSN AND a.MONTH = b.MONTH
""")
# END OF SUBCHUNK 7
# BEGIN SUBCHUNK 8
# SUBCHUNK 8: Summarize base transfer-out, then join with NEW_LINES3 for updated transfer metrics

spark.sql("""
    CREATE OR REPLACE TEMP VIEW BASE_XFER_OUT AS
    SELECT
        TRAIN_START_MONTH_1 AS MONTH,
        POSN,
        SUM(PBT) AS BASE_TRANSFER_OUT
    FROM BASE_XFER_INFO
    GROUP BY TRAIN_START_MONTH_1, POSN
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW NEW_BASE_XFER_OUT AS
    SELECT
        a.MONTH,
        a.ACFT,
        a.SEAT,
        a.DOM,
        a.AVAIL,
        a.FLX_FLYING_LINE,
        a.TOT_AVAIL,
        a.RETIRE,
        a.BASE_TRANSFER_IN,
        b.BASE_TRANSFER_OUT,
        a.BASE_TRANSFER_IN + b.BASE_TRANSFER_OUT AS BASE_TRANSFER,
        a.TRANSFER_OUT,
        a.TRANSFER_IN,
        a.NH_ACTIVATED,
        a.FLX_TO_LINE,
        a.NET_CHANGE,
        a.POSN,
        a.TYPE
    FROM NEW_LINES3 a
    INNER JOIN BASE_XFER_OUT b
        ON a.MONTH = b.MONTH AND a.POSN = b.POSN
    ORDER BY a.POSN, a.MONTH
""")
# END OF SUBCHUNK 8
# BEGIN SUBCHUNK 9
# Docstring: This chunk updates the table NEW_LINES4 by merging NEW_LINES3 with NEW_BASE_XFER_OUT using POSN and MONTH as keys. Rows from NEW_BASE_XFER_OUT update matching rows in NEW_LINES3, and unmatched rows are appended, replicating SAS UPDATE logic.

spark.sql("""
CREATE OR REPLACE TEMP VIEW merged_updates AS
SELECT
    COALESCE(b.POSN, a.POSN) AS POSN,
    COALESCE(b.MONTH, a.MONTH) AS MONTH,
    -- Take columns from NEW_BASE_XFER_OUT (b) if present, otherwise from NEW_LINES3 (a)
    -- List all columns present in both tables, prefer b's values if not null, else a's values
    COALESCE(b.BASE_TRANSFER, a.BASE_TRANSFER) AS BASE_TRANSFER,
    COALESCE(b.BASE_TRANSFER_IN, a.BASE_TRANSFER_IN) AS BASE_TRANSFER_IN,
    COALESCE(b.BASE_TRANSFER_OUT, a.BASE_TRANSFER_OUT) AS BASE_TRANSFER_OUT,
    COALESCE(b.RETIREMENT, a.RETIREMENT) AS RETIREMENT,
    COALESCE(b.PRIOR_LINES, a.PRIOR_LINES) AS PRIOR_LINES,
    COALESCE(b.LINE, a.LINE) AS LINE,
    COALESCE(b.LINE_TYPE, a.LINE_TYPE) AS LINE_TYPE,
    COALESCE(b.LINE_TITLE, a.LINE_TITLE) AS LINE_TITLE,
    COALESCE(b.LINE_TIMESTAMP, a.LINE_TIMESTAMP) AS LINE_TIMESTAMP
FROM WORK.NEW_LINES3 a
FULL OUTER JOIN WORK.NEW_BASE_XFER_OUT b
    ON a.POSN = b.POSN AND a.MONTH = b.MONTH
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_LINES4 AS
SELECT * FROM merged_updates
""")
# END OF SUBCHUNK 9
# BEGIN SUBCHUNK 10
# Create WORK.XFERS
spark.sql("""
CREATE OR REPLACE TEMP VIEW WORK_XFERS AS
SELECT
  EMP_NBR,
  TRAIN_START_MONTH_1,
  TRAIN_END_MONTH_1,
  ACFT,
  SEAT,
  DOM,
  POSN,
  TRAIN_POS_1,
  CUR_POS_1,
  -1 AS `OUT`,
  1 AS `IN`
FROM WORK_CAM_RESULTS
WHERE SUBSTR(TRAIN_POS_1, 1, 3) <> SUBSTR(CUR_POS_1, 1, 3)
  AND POSN = CUR_POS_1
ORDER BY EMP_NBR
""")

# Create WORK.XFER_N_RET
spark.sql("""
CREATE OR REPLACE TEMP VIEW WORK_XFER_N_RET AS
SELECT
  a.EMP_NBR,
  a.TRAIN_START_MONTH_1,
  a.TRAIN_END_MONTH_1,
  a.ACFT,
  a.SEAT,
  a.DOM,
  a.POSN,
  a.TRAIN_POS_1,
  a.CUR_POS_1,
  a.`OUT`,
  a.`IN`,
  b.`MONTH`
FROM WORK_XFERS a, WORK_RETIRE_INFO b
WHERE (a.EMP_NBR = b.EMP_NBR AND a.TRAIN_START_MONTH_1 > b.`MONTH`)
   OR (a.EMP_NBR = b.EMP_NBR AND a.TRAIN_END_MONTH_1 > b.`MONTH`)
ORDER BY a.EMP_NBR
""")

def transfer_in(obs: int):
    """
    Macro transfer_in converted to Python.
    Executes subsequent transfer-in logic only when obs > 0.
    This function will be populated by later subchunks with SQL-first operations.
    """
    if obs > 0:
        # Placeholder for subsequent DO-block logic from later subchunks
        pass
# END OF SUBCHUNK 10
# BEGIN SUBCHUNK 11
# SUBCHUNK 11: Select distinct EMP_NBR from WORK.XFER_N_RET into a Python variable as a space-separated string (equivalent to SAS macro variable)
/*
This step extracts unique employee numbers (EMP_NBR) from XFER_N_RET for later logic, and stores them in a Python variable named 'retired', separated by spaces.
*/

# Create or replace temp view for XFER_N_RET if not already done
# (Assumes table already exists from earlier steps)

retired_emp_nbrs = spark.sql("""
    SELECT DISTINCT EMP_NBR
    FROM XFER_N_RET
""").rdd.flatMap(lambda x: x).collect()
retired = ' '.join(str(emp) for emp in retired_emp_nbrs)
# END OF SUBCHUNK 11
# BEGIN SUBCHUNK 12
# SUBCHUNK 12: Creates XFER_INFO (filtered XFERS), XFER_IN (aggregated transfer-ins), and NEW_XFER_IN (joined NEW_LINES4 with transfer-in data) as temp views for subsequent analysis.

# Create XFER_INFO by filtering out retired EMP_NBRs
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW XFER_INFO AS
SELECT
  EMP_NBR, 
  TRAIN_START_MONTH_1, 
  TRAIN_END_MONTH_1,
  ACFT, 
  SEAT,
  DOM,
  POSN,
  TRAIN_POS_1,
  CUR_POS_1,
  OUT,
  IN
FROM XFERS
WHERE EMP_NBR NOT IN ({retired})
ORDER BY EMP_NBR
""")

# Create XFER_IN by aggregating transfer-ins by month and position
spark.sql("""
CREATE OR REPLACE TEMP VIEW XFER_IN AS
SELECT DISTINCT
  TRAIN_END_MONTH_1 AS MONTH,
  TRAIN_POS_1 AS POSN,
  SUM(IN) AS TRANSFER_IN
FROM XFER_INFO
GROUP BY TRAIN_END_MONTH_1, TRAIN_POS_1
""")

# Join NEW_LINES4 and XFER_IN for NEW_XFER_IN summary
spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_XFER_IN AS
SELECT DISTINCT
  a.MONTH, 
  a.ACFT, 
  a.SEAT, 
  a.DOM,
  a.AVAIL,
  a.FLX_FLYING_LINE,
  a.TOT_AVAIL,
  a.RETIRE, 
  a.BASE_TRANSFER_IN, 
  a.BASE_TRANSFER_OUT,
  a.BASE_TRANSFER,
  a.TRANSFER_OUT, 
  b.TRANSFER_IN, 
  a.NH_ACTIVATED, 
  a.FLX_TO_LINE, 
  a.NET_CHANGE, 
  a.POSN,
  a.TYPE 
FROM NEW_LINES4 a
JOIN XFER_IN b
  ON a.MONTH = b.MONTH AND a.POSN = b.POSN
ORDER BY a.POSN, a.MONTH
""")
# END OF SUBCHUNK 12
# BEGIN SUBCHUNK 13
# SUBCHUNK 13: Convert SAS PROC SQL steps to Spark SQL, maintaining table/view names for in-place updates

spark.sql("""
CREATE OR REPLACE TEMP VIEW XFER_INFO AS
SELECT
  EMP_NBR,
  TRAIN_START_MONTH_1,
  TRAIN_END_MONTH_1,
  ACFT,
  SEAT,
  DOM,
  POSN,
  TRAIN_POS_1,
  CUR_POS_1,
  OUT,
  IN
FROM XFERS
ORDER BY EMP_NBR
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW XFER_IN AS
SELECT
  TRAIN_END_MONTH_1 AS MONTH,
  TRAIN_POS_1 AS POSN,
  SUM(IN) AS TRANSFER_IN
FROM XFER_INFO
GROUP BY TRAIN_END_MONTH_1, TRAIN_POS_1
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_XFER_IN AS
SELECT DISTINCT
  a.MONTH,
  a.ACFT,
  a.SEAT,
  a.DOM,
  a.AVAIL,
  a.FLX_FLYING_LINE,
  a.TOT_AVAIL,
  a.RETIRE,
  a.BASE_TRANSFER_IN,
  a.BASE_TRANSFER_OUT,
  a.BASE_TRANSFER,
  a.TRANSFER_OUT,
  b.TRANSFER_IN,
  a.NH_ACTIVATED,
  a.FLX_TO_LINE,
  a.NET_CHANGE,
  a.POSN,
  a.TYPE
FROM NEW_LINES4 a
JOIN XFER_IN b
  ON a.MONTH = b.MONTH AND a.POSN = b.POSN
ORDER BY a.POSN, a.MONTH
""")
# END OF SUBCHUNK 13
#END OF CHUNK 3#
#BEGIN CHUNK 4#
# BEGIN SUBCHUNK 1
def transfer_in():
    """
    Executes the logic equivalent to the SAS '%transfer_in' macro.
    The exact operations depend on the macro's full definition, which is not shown here.
    If the macro involves Spark SQL transformations or DataFrame steps,
    those should be implemented inside this function using spark.sql() calls and DataFrame API as required.
    """
    # SKIPPED: Macro body not provided. Implement macro logic here when available.
# END OF SUBCHUNK 1
# BEGIN SUBCHUNK 2
# This chunk updates the NEW_LINES5 dataset by merging NEW_LINES4 and NEW_XFER_IN using POSN and MONTH as keys, mimicking SAS UPDATE logic.

spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_LINES5 AS
WITH merged AS (
  SELECT
    COALESCE(a.POSN, b.POSN) AS POSN,
    COALESCE(a.MONTH, b.MONTH) AS MONTH,
    -- Select columns from NEW_LINES4 (a) and update with NEW_XFER_IN (b) where not null
    -- Replace the below with the actual column list and update logic as needed
    COALESCE(b.col1, a.col1) AS col1,
    COALESCE(b.col2, a.col2) AS col2,
    COALESCE(b.col3, a.col3) AS col3
    -- Add additional columns as required
  FROM NEW_LINES4 a
  FULL OUTER JOIN NEW_XFER_IN b
    ON a.POSN = b.POSN AND a.MONTH = b.MONTH
)
SELECT * FROM merged
ORDER BY POSN, MONTH
""")
# END OF SUBCHUNK 2
# BEGIN SUBCHUNK 3
# The following chunk creates a transfer-out summary table, then joins it to an in-progress forecast table, materializing the result for downstream steps.
# It uses only Spark SQL, with temp views for WORK library emulation.

# Create or replace a temp view for XFER_OUT (transfer out summary by month/position)
spark.sql("""
CREATE OR REPLACE TEMP VIEW XFER_OUT AS
SELECT
  TRAIN_START_MONTH_1 AS MONTH,
  CUR_POS_1 AS POSN,
  SUM(OUT) AS TRANSFER_OUT
FROM XFER_INFO
GROUP BY TRAIN_START_MONTH_1, CUR_POS_1
""")

# Create or replace a temp view for NEW_XFER_OUT by joining NEW_LINES5 and XFER_OUT
spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_XFER_OUT AS
SELECT
  a.MONTH,
  a.ACFT,
  a.SEAT,
  a.DOM,
  a.AVAIL,
  a.FLX_FLYING_LINE,
  a.TOT_AVAIL,
  a.RETIRE,
  a.BASE_TRANSFER_IN,
  a.BASE_TRANSFER_OUT,
  a.BASE_TRANSFER,
  b.TRANSFER_OUT,
  a.TRANSFER_IN,
  a.NH_ACTIVATED,
  a.FLX_TO_LINE,
  a.NET_CHANGE,
  a.POSN,
  a.TYPE
FROM NEW_LINES5 a
JOIN XFER_OUT b
  ON a.MONTH = b.MONTH AND a.POSN = b.POSN
ORDER BY a.POSN, a.MONTH
""")
# END OF SUBCHUNK 3
# BEGIN SUBCHUNK 4
# SUBCHUNK 4: Update NEW_LINES6 by merging NEW_LINES5 and NEW_XFER_OUT on POSN and MONTH
spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_LINES6 AS
SELECT
    COALESCE(b.POSN, a.POSN) AS POSN,
    COALESCE(b.MONTH, a.MONTH) AS MONTH,
    -- Select all columns from NEW_LINES5 (a)
    a.*,
    -- Overwrite columns from NEW_XFER_OUT (b) where present (except POSN, MONTH)
    b.*
FROM NEW_LINES5 a
FULL OUTER JOIN NEW_XFER_OUT b
    ON a.POSN = b.POSN AND a.MONTH = b.MONTH
""")
# The FULL OUTER JOIN ensures all rows from both tables are included, with NEW_XFER_OUT overwriting NEW_LINES5 where matches occur.
# END OF SUBCHUNK 4
# BEGIN SUBCHUNK 5
# SUBCHUNK 5: SAS PROC SQL conversion to Spark SQL for new hire info, aggregation, and join with line forecast.
spark.sql("""
CREATE OR REPLACE TEMP VIEW NEWHIRE_INFO AS
SELECT
    EMP_NBR,
    TRAIN_START_MONTH_1,
    TRAIN_END_MONTH_1 AS MONTH,
    ACFT,
    SEAT,
    DOM,
    POSN,
    TRAIN_POS_1,
    CUR_POS_1,
    PRIM_CLASS_CD
FROM CAM_RESULTS
WHERE PRIM_CLASS_CD = 'STU'
ORDER BY MONTH, EMP_NBR
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW NEWHIRE AS
SELECT
    POSN,
    MONTH,
    ACFT,
    SEAT,
    DOM,
    COUNT(EMP_NBR) AS NH_ACTIVATED
FROM NEWHIRE_INFO
GROUP BY POSN, MONTH, ACFT, SEAT, DOM
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_NEWHIRE AS
SELECT DISTINCT
    a.MONTH,
    a.ACFT,
    a.SEAT,
    a.DOM,
    a.AVAIL,
    a.FLX_FLYING_LINE,
    a.TOT_AVAIL,
    a.RETIRE,
    a.BASE_TRANSFER_IN,
    a.BASE_TRANSFER_OUT,
    a.BASE_TRANSFER,
    a.TRANSFER_OUT,
    a.TRANSFER_IN,
    b.NH_ACTIVATED,
    a.FLX_TO_LINE,
    a.NET_CHANGE,
    a.POSN,
    a.TYPE
FROM NEW_LINES6 a
JOIN NEWHIRE b
  ON a.MONTH = b.MONTH
 AND a.POSN = b.POSN
ORDER BY a.POSN, a.MONTH
""")
# END OF SUBCHUNK 5
# BEGIN SUBCHUNK 6
# SUBCHUNK 6: Updates NEW_LINES6 with NEW_NEWHIRE on POSN and MONTH to create NEW_LINES7
spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_LINES7 AS
SELECT
    COALESCE(b.POSN, a.POSN) AS POSN,
    COALESCE(b.MONTH, a.MONTH) AS MONTH,
    -- Select all columns from NEW_NEWHIRE if present, otherwise from NEW_LINES6
    COALESCE(b.NH_ACTIVATED, a.NH_ACTIVATED) AS NH_ACTIVATED,
    COALESCE(b.OTHER_COL1, a.OTHER_COL1) AS OTHER_COL1,
    COALESCE(b.OTHER_COL2, a.OTHER_COL2) AS OTHER_COL2
FROM
    NEW_LINES6 a
    FULL OUTER JOIN NEW_NEWHIRE b
        ON a.POSN = b.POSN AND a.MONTH = b.MONTH
""")
# END OF SUBCHUNK 6
# BEGIN SUBCHUNK 7
# Creates CAM_SUMMARY_DATA with UNIQUE_ID, MONTH, and VALUE renamed as POSITION, ordered by MONTH
spark.sql("""
    CREATE OR REPLACE TEMP VIEW CAM_SUMMARY_DATA AS
    SELECT
        UNIQUE_ID,
        MONTH,
        VALUE AS POSITION
    FROM CAM_MONTHS
    ORDER BY MONTH
""")
# END OF SUBCHUNK 7
# BEGIN SUBCHUNK 8
# SUBCHUNK 8: Frequency table for MONTH*POSITION; output counts only (no percents, cumulative, col/row/sparse)
spark.sql("""
    CREATE OR REPLACE TABLE WORK.REPORT_DATA_1 AS
    SELECT
        MONTH,
        POSITION,
        COUNT(*) AS COUNT
    FROM WORK.CAM_SUMMARY_DATA
    WHERE MONTH IS NOT NULL AND POSITION IS NOT NULL
    GROUP BY MONTH, POSITION
""")
# END OF SUBCHUNK 8
# BEGIN SUBCHUNK 9
# SUBCHUNK 9: Converts SAS PROC SQL logic to sequential Spark SQL temp views and tables for report and availability datasets

spark.sql("""
    CREATE OR REPLACE TEMP VIEW REPORT_DATA_2 AS
    SELECT
        MONTH,
        POSITION,
        COUNT AS AVAIL
    FROM REPORT_DATA_1
    WHERE POSITION IS NOT NULL
    ORDER BY MONTH, POSITION
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW FLX_FLY_LINE AS
    SELECT
        MONTH,
        POSN,
        FLX_FLYING_LINE
    FROM FO_SRC_FLX_FLYING_LINE
    ORDER BY MONTH, POSN
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW AVAIL AS
    SELECT DISTINCT
        a.MONTH,
        a.ACFT,
        a.SEAT,
        a.DOM,
        b.AVAIL,
        a.FLX_FLYING_LINE,
        a.TOT_AVAIL,
        a.RETIRE,
        a.BASE_TRANSFER_IN,
        a.BASE_TRANSFER_OUT,
        a.BASE_TRANSFER,
        a.TRANSFER_OUT,
        a.TRANSFER_IN,
        a.NH_ACTIVATED,
        a.FLX_TO_LINE,
        a.NET_CHANGE,
        a.POSN,
        a.TYPE
    FROM NEW_LINES7 a
    INNER JOIN REPORT_DATA_2 b
      ON a.MONTH = b.MONTH AND a.POSN = b.POSITION
    ORDER BY a.POSN, a.MONTH
""")
# END OF SUBCHUNK 9
# BEGIN SUBCHUNK 10
# This chunk creates NEW_LINES8 by updating NEW_LINES7 with AVAIL, merging by POSN and MONTH.

spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_LINES8 AS
SELECT
    COALESCE(b.POSN, a.POSN) AS POSN,
    COALESCE(b.MONTH, a.MONTH) AS MONTH,
    -- Merge all columns from both datasets, preferring AVAIL values where present
    COALESCE(b.AVAIL, a.AVAIL) AS AVAIL,
    -- Add other columns from NEW_LINES7 and AVAIL that are not duplicated
    a.* EXCEPT(POSN, MONTH, AVAIL),
    b.* EXCEPT(POSN, MONTH, AVAIL)
FROM NEW_LINES7 a
FULL OUTER JOIN AVAIL b
    ON a.POSN = b.POSN AND a.MONTH = b.MONTH
ORDER BY POSN, MONTH
""")
# END OF SUBCHUNK 10
# BEGIN SUBCHUNK 11
# This chunk creates WORK.FLX_FLY as a distinct join of NEW_LINES8 and FLX_FLY_LINE on MONTH and POSN, calculating TOT_AVAIL and selecting relevant columns.

spark.sql("""
CREATE OR REPLACE TEMP VIEW FLX_FLY AS
SELECT DISTINCT
  a.MONTH,
  a.ACFT,
  a.SEAT,
  a.DOM,
  a.AVAIL,
  b.FLX_FLYING_LINE,
  (b.FLX_FLYING_LINE + a.AVAIL) AS TOT_AVAIL,
  a.RETIRE,
  a.BASE_TRANSFER_IN,
  a.BASE_TRANSFER_OUT,
  a.BASE_TRANSFER,
  a.TRANSFER_OUT,
  a.TRANSFER_IN,
  a.NH_ACTIVATED,
  a.FLX_TO_LINE,
  a.NET_CHANGE,
  a.POSN,
  a.TYPE
FROM NEW_LINES8 a
JOIN FLX_FLY_LINE b
  ON a.MONTH = b.MONTH
 AND a.POSN = b.POSN
ORDER BY a.POSN, a.MONTH
""")
# END OF SUBCHUNK 11
# BEGIN SUBCHUNK 12
# This block creates NEW_LINES9 by merging (updating) NEW_LINES8 with FLX_FLY on POSN and MONTH.
spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_LINES9 AS
SELECT
    COALESCE(f.POSN, n.POSN) AS POSN,
    COALESCE(f.MONTH, n.MONTH) AS MONTH,
    COALESCE(f.ACFT, n.ACFT) AS ACFT,
    COALESCE(f.SEAT, n.SEAT) AS SEAT,
    COALESCE(f.DOM, n.DOM) AS DOM,
    COALESCE(f.AVAIL, n.AVAIL) AS AVAIL,
    COALESCE(f.RETIRE, n.RETIRE) AS RETIRE,
    COALESCE(f.BASE_TRANSFER, n.BASE_TRANSFER) AS BASE_TRANSFER,
    COALESCE(f.TRANSFER_OUT, n.TRANSFER_OUT) AS TRANSFER_OUT,
    COALESCE(f.TRANSFER_IN, n.TRANSFER_IN) AS TRANSFER_IN,
    COALESCE(f.NH_ACTIVATED, n.NH_ACTIVATED) AS NH_ACTIVATED,
    COALESCE(f.FLX_TO_LINE, n.FLX_TO_LINE) AS FLX_TO_LINE,
    COALESCE(f.FLX_FLYING_LINE, n.FLX_FLYING_LINE) AS FLX_FLYING_LINE,
    COALESCE(f.TOT_AVAIL, n.TOT_AVAIL) AS TOT_AVAIL
FROM NEW_LINES8 n
FULL OUTER JOIN FLX_FLY f
    ON n.POSN = f.POSN AND n.MONTH = f.MONTH
""")
# END OF SUBCHUNK 12
# BEGIN SUBCHUNK 13
# The following code creates the WORK.FORECAST table by selecting distinct rows from WORK.NEW_LINES9, computing TOT_AVAIL and NET_CHANGE as per the SAS logic, and ordering the result by POSN and MONTH.
spark.sql("""
CREATE OR REPLACE TABLE WORK.FORECAST AS
SELECT DISTINCT
    a.MONTH,
    a.ACFT,
    a.SEAT,
    a.DOM,
    a.AVAIL,
    a.FLX_FLYING_LINE,
    CASE WHEN a.FLX_FLYING_LINE = 0 THEN a.AVAIL ELSE a.TOT_AVAIL END AS TOT_AVAIL,
    a.RETIRE,
    a.BASE_TRANSFER_IN,
    a.BASE_TRANSFER_OUT,
    a.BASE_TRANSFER,
    a.TRANSFER_OUT,
    a.TRANSFER_IN,
    a.NH_ACTIVATED,
    a.FLX_TO_LINE,
    (a.RETIRE + a.BASE_TRANSFER + a.TRANSFER_OUT + a.TRANSFER_IN + a.NH_ACTIVATED + a.FLX_TO_LINE) AS NET_CHANGE,
    a.POSN,
    a.TYPE
FROM WORK.NEW_LINES9 a
ORDER BY POSN, MONTH
""")
# END OF SUBCHUNK 13
# BEGIN SUBCHUNK 14
# SUBCHUNK 14: Extracts 'timestamp' and 'title' from WORK.SCENARIOS for the given row number (&i) and assigns to Python variables.
row = i  # Assuming 'i' is defined elsewhere as an integer

tmstp_query = f"""
SELECT timestamp
FROM WORK.SCENARIOS
WHERE row = {row}
"""

title_query = f"""
SELECT title
FROM WORK.SCENARIOS
WHERE row = {row}
"""

tmstp_df = spark.sql(tmstp_query)
tmstp = tmstp_df.collect()[0][0] if tmstp_df.count() > 0 else None

title_df = spark.sql(title_query)
title = title_df.collect()[0][0] if title_df.count() > 0 else None
# END OF SUBCHUNK 14
# BEGIN SUBCHUNK 15
"""SUBCHUNK 15: Create working tables for CAM results, months, distinct months and positions, and initialize NEW_LINES1 with cross-join of months and positions, default metrics, and scenario title. Also capture min/max month values for later use."""
# 
# Prepare safe SQL literals for variables previously set
title_sql = title.replace("'", "''")
tmstp_sql = tmstp

spark.sql(f"""
CREATE OR REPLACE TABLE work.CAM_RESULTS AS
WITH base AS (
  SELECT
    UNIQUE_ID,
    UNIQUE_ID AS ID,
    EMP_NBR,
    EMP_NM,
    SENIORITY_NBR,
    FLY_POSITION,
    CASE WHEN FLY_POSITION <> 'N/A' THEN FLY_POSITION ELSE CUR_POS_1 END AS POSN,
    AGE_65_DT,
    PRIM_CLASS_CD,
    SEC_CLASS_CD,
    AWARD_TYPE_CD,
    DEF_PRIM_CLASS_CD,
    DEF_SEC_CLASS_CD,
    DEF_AWARD_TYPE_CD,
    DEF_POS,
    DIRECTION,
    DEFERRED_BIDMONTH,
    RETIRE_DT,
    RETIRE_MONTH,
    TRAIN_POS_1,
    CUR_POS_1,
    TRANS_UPG_CD_1,
    START_DT_1,
    VIPS_END_DT_1,
    TRAIN_START_MONTH_1,
    END_DT_1,
    TRAIN_END_MONTH_1,
    TRAIN_SOURCE_1,
    TRAIN_POS_2,
    CUR_POS_2,
    TRANS_UPG_CD_2,
    START_DT_2,
    VIPS_END_DT_2,
    TRAIN_START_MONTH_2,
    END_DT_2,
    TRAIN_END_MONTH_2,
    TRAIN_SOURCE_2
  FROM cam.CAM_RESULTS_DETAILS
  WHERE TIMESTAMP = '{tmstp_sql}'
)
SELECT
  ID,
  EMP_NBR,
  EMP_NM,
  SENIORITY_NBR,
  FLY_POSITION,
  POSN,
  AGE_65_DT,
  PRIM_CLASS_CD,
  SEC_CLASS_CD,
  AWARD_TYPE_CD,
  DEF_PRIM_CLASS_CD,
  DEF_SEC_CLASS_CD,
  DEF_AWARD_TYPE_CD,
  DEF_POS,
  DIRECTION,
  DEFERRED_BIDMONTH,
  RETIRE_DT,
  RETIRE_MONTH,
  TRAIN_POS_1,
  CUR_POS_1,
  TRANS_UPG_CD_1,
  START_DT_1,
  VIPS_END_DT_1,
  TRAIN_START_MONTH_1,
  END_DT_1,
  TRAIN_END_MONTH_1,
  TRAIN_SOURCE_1,
  TRAIN_POS_2,
  CUR_POS_2,
  TRANS_UPG_CD_2,
  START_DT_2,
  VIPS_END_DT_2,
  TRAIN_START_MONTH_2,
  END_DT_2,
  TRAIN_END_MONTH_2,
  TRAIN_SOURCE_2,
  SUBSTR(POSN, 1, 2) AS ACFT,
  CASE WHEN SUBSTR(POSN, 3, 1) = 'C' THEN 'CAP' ELSE 'FO' END AS SEAT,
  CASE
    WHEN SUBSTR(POSN, 4, 1) = 'M' THEN 'MEM'
    WHEN SUBSTR(POSN, 4, 1) = 'I' THEN 'IND'
    WHEN SUBSTR(POSN, 4, 1) = 'E' THEN 'EUR'
    WHEN SUBSTR(POSN, 4, 1) = 'A' THEN 'ANC'
    WHEN SUBSTR(POSN, 4, 1) = 'H' THEN 'HKG'
    WHEN SUBSTR(POSN, 4, 1) = 'L' THEN 'LAX'
    WHEN SUBSTR(POSN, 4, 1) = 'O' THEN 'OAK'
    ELSE ' '
  END AS DOM
FROM base
ORDER BY ID
""")

spark.sql(f"""
CREATE OR REPLACE TABLE work.CAM_MONTHS AS
SELECT
  UNIQUE_ID,
  MONTH,
  VALUE
FROM cam.CAM_RESULTS_MONTHS
WHERE TIMESTAMP = '{tmstp_sql}'
ORDER BY UNIQUE_ID, MONTH
""")

spark.sql("""
CREATE OR REPLACE TABLE work.MONTHS AS
SELECT DISTINCT
  MONTH
FROM work.CAM_MONTHS
ORDER BY MONTH
""")

spark.sql("""
CREATE OR REPLACE TABLE work.POSN AS
SELECT DISTINCT
  POSN,
  ACFT,
  SEAT,
  DOM
FROM work.CAM_RESULTS
WHERE POSN NOT IN ('10CM','27CM','27FM','27SM')
ORDER BY POSN
""")

spark.sql(f"""
CREATE OR REPLACE TABLE work.NEW_LINES1 AS
SELECT
  CAST(a.MONTH AS DATE) AS MONTH,
  b.ACFT,
  b.SEAT,
  b.DOM,
  0 AS AVAIL,
  0 AS FLX_FLYING_LINE,
  0 AS TOT_AVAIL,
  0 AS RETIRE,
  0 AS BASE_TRANSFER_IN,
  0 AS BASE_TRANSFER_OUT,
  0 AS BASE_TRANSFER,
  0 AS TRANSFER_OUT,
  0 AS TRANSFER_IN,
  0 AS NH_ACTIVATED,
  0 AS FLX_TO_LINE,
  0 AS NET_CHANGE,
  b.POSN,
  CAST('{title_sql}' AS STRING) AS TYPE
FROM work.MONTHS a
CROSS JOIN work.POSN b
ORDER BY b.POSN, a.MONTH
""")

# Macro variable equivalents: assign min/max month values
min_mth = spark.sql("SELECT MIN(MONTH) AS min_mth FROM work.CAM_MONTHS").collect()[0]["min_mth"]
max_mth = spark.sql("SELECT MAX(MONTH) AS max_mth FROM work.CAM_MONTHS").collect()[0]["max_mth"]
#
# END OF SUBCHUNK 15
# BEGIN SUBCHUNK 16
"""SUBCHUNK 16: Create retiree-level info, aggregate retire counts by position and month, and join to baseline lines. All steps are expressed as Spark SQL CTAS into WORK schema to preserve SAS dataset names. Assumes Python variables min_mth and max_mth (DATE literals) are already set from prior steps."""

# 
spark.sql(f"""
CREATE OR REPLACE TABLE WORK.RETIRE_INFO AS
WITH base AS (
  SELECT
    EMP_NBR,
    RETIRE_MONTH AS MONTH,
    CASE
      WHEN TRAIN_POS_1 IS NULL THEN POSN
      WHEN TRAIN_POS_1 IS NOT NULL AND START_DT_1 > RETIRE_DT THEN POSN
      WHEN TRAIN_POS_1 IS NOT NULL AND START_DT_1 <= RETIRE_DT AND RETIRE_DT <= END_DT_1 THEN POSN
      ELSE TRAIN_POS_1
    END AS POSN
  FROM WORK.CAM_RESULTS
  WHERE RETIRE_MONTH BETWEEN CAST('{min_mth}' AS DATE) AND CAST('{max_mth}' AS DATE)
)
SELECT
  EMP_NBR,
  MONTH,
  POSN,
  SUBSTR(POSN, 1, 2) AS ACFT,
  CASE WHEN SUBSTR(POSN, 3, 1) = 'C' THEN 'CAP' ELSE 'FO' END AS SEAT,
  CASE
    WHEN SUBSTR(POSN, 4, 1) = 'M' THEN 'MEM'
    WHEN SUBSTR(POSN, 4, 1) = 'I' THEN 'IND'
    WHEN SUBSTR(POSN, 4, 1) = 'E' THEN 'EUR'
    WHEN SUBSTR(POSN, 4, 1) = 'A' THEN 'ANC'
    WHEN SUBSTR(POSN, 4, 1) = 'H' THEN 'HKG'
    WHEN SUBSTR(POSN, 4, 1) = 'L' THEN 'LAX'
    WHEN SUBSTR(POSN, 4, 1) = 'O' THEN 'OAK'
    ELSE ' '
  END AS DOM,
  -1 AS RET
FROM base
ORDER BY EMP_NBR
""")

spark.sql("""
CREATE OR REPLACE TABLE WORK.RETIRE AS
SELECT DISTINCT
  POSN,
  MONTH,
  ACFT,
  SEAT,
  DOM,
  SUM(RET) AS RETIRE
FROM WORK.RETIRE_INFO
GROUP BY POSN, MONTH, ACFT, SEAT, DOM
""")

spark.sql("""
CREATE OR REPLACE TABLE WORK.NEW_REITRE AS
SELECT DISTINCT
  CAST(a.MONTH AS DATE) AS MONTH,
  a.ACFT,
  a.SEAT,
  a.DOM,
  a.AVAIL,
  a.FLX_FLYING_LINE,
  a.TOT_AVAIL,
  b.RETIRE,
  a.BASE_TRANSFER_IN,
  a.BASE_TRANSFER_OUT,
  a.BASE_TRANSFER,
  a.TRANSFER_OUT,
  a.TRANSFER_IN,
  a.NH_ACTIVATED,
  a.FLX_TO_LINE,
  a.NET_CHANGE,
  a.POSN,
  a.TYPE
FROM WORK.NEW_LINES1 a
JOIN WORK.RETIRE b
  ON a.MONTH = b.MONTH
 AND a.POSN  = b.POSN
ORDER BY POSN, MONTH
""")
# SKIPPED: PROC SQL QUIT has no executable equivalent in Spark
#
# END OF SUBCHUNK 16
# BEGIN SUBCHUNK 17
# SUBCHUNK 17: Update NEW_LINES1 with NEW_REITRE by POSN and MONTH to produce NEW_LINES2

spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_LINES2 AS
SELECT
    COALESCE(r.POSN, n.POSN) AS POSN,
    COALESCE(r.MONTH, n.MONTH) AS MONTH,
    -- Select columns from NEW_REITRE if present, else from NEW_LINES1
    COALESCE(r.TITLE, n.TITLE) AS TITLE,
    COALESCE(r.RET, n.RET) AS RET,
    COALESCE(r.ACFT, n.ACFT) AS ACFT,
    COALESCE(r.SEAT, n.SEAT) AS SEAT,
    COALESCE(r.DOM, n.DOM) AS DOM,
    COALESCE(r.FLYING_LINES, n.FLYING_LINES) AS FLYING_LINES,
    COALESCE(r.BASE_TRANSFER_IN, n.BASE_TRANSFER_IN) AS BASE_TRANSFER_IN,
    COALESCE(r.BASE_TRANSFER_OUT, n.BASE_TRANSFER_OUT) AS BASE_TRANSFER_OUT
FROM NEW_LINES1 n
LEFT JOIN NEW_REITRE r
  ON n.POSN = r.POSN AND n.MONTH = r.MONTH
UNION ALL
SELECT
    r.POSN, r.MONTH, r.TITLE, r.RET, r.ACFT, r.SEAT, r.DOM, r.FLYING_LINES, r.BASE_TRANSFER_IN, r.BASE_TRANSFER_OUT
FROM NEW_REITRE r
LEFT ANTI JOIN NEW_LINES1 n
  ON r.POSN = n.POSN AND r.MONTH = n.MONTH
""")
# END OF SUBCHUNK 17
# BEGIN SUBCHUNK 18
# 
# This chunk creates BASE_XFER_INFO by filtering CAM_RESULTS for specific substring logic and mapping, 
# then aggregates it to BASE_XFER_IN by month/position, and finally joins with NEW_LINES2 to create NEW_BASE_XFER_IN 
# with transfer calculations for each position and month.

spark.sql("""
    CREATE OR REPLACE TEMP VIEW BASE_XFER_INFO AS
    SELECT
        EMP_NBR, 
        TRAIN_START_MONTH_1, 
        TRAIN_END_MONTH_1,
        ACFT, 
        SEAT,
        DOM,
        POSN,
        TRAIN_POS_1,
        CUR_POS_1,
        -1 AS PBT,
        1 AS BT
    FROM CAM_RESULTS
    WHERE SUBSTR(TRAIN_POS_1,1,3) = SUBSTR(CUR_POS_1,1,3)
      AND SUBSTR(TRAIN_POS_1,4,1) <> SUBSTR(CUR_POS_1,4,1)
      AND POSN = CUR_POS_1
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW BASE_XFER_IN AS
    SELECT DISTINCT
        TRAIN_END_MONTH_1 AS MONTH,
        TRAIN_POS_1 AS POSN,
        SUM(BT) AS BASE_TRANSFER_IN
    FROM BASE_XFER_INFO
    GROUP BY TRAIN_END_MONTH_1, TRAIN_POS_1
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW NEW_BASE_XFER_IN AS
    SELECT DISTINCT
        a.MONTH, 
        a.ACFT, 
        a.SEAT, 
        a.DOM,
        a.AVAIL,
        a.FLX_FLYING_LINE,
        a.TOT_AVAIL, 
        a.RETIRE, 
        b.BASE_TRANSFER_IN, 
        a.BASE_TRANSFER_OUT,
        b.BASE_TRANSFER_IN + a.BASE_TRANSFER_OUT AS BASE_TRANSFER,
        a.TRANSFER_OUT, 
        a.TRANSFER_IN, 
        a.NH_ACTIVATED, 
        a.FLX_TO_LINE, 
        a.NET_CHANGE, 
        a.POSN,
        a.TYPE 
    FROM NEW_LINES2 a
    INNER JOIN BASE_XFER_IN b
      ON a.MONTH = b.MONTH AND a.POSN = b.POSN
    ORDER BY POSN, MONTH
""")
#
# END OF SUBCHUNK 18
# BEGIN SUBCHUNK 19
# This chunk updates NEW_LINES2 with NEW_BASE_XFER_IN based on POSN and MONTH and writes the result to NEW_LINES3.

spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_LINES3 AS
SELECT
    COALESCE(b.POSN, a.POSN) AS POSN,
    COALESCE(b.MONTH, a.MONTH) AS MONTH,
    COALESCE(b.BASE_TRANSFER_IN, a.BASE_TRANSFER_IN) AS BASE_TRANSFER_IN,
    COALESCE(b.BASE_TRANSFER_OUT, a.BASE_TRANSFER_OUT) AS BASE_TRANSFER_OUT,
    COALESCE(b.BASE_TRANSFER, a.BASE_TRANSFER) AS BASE_TRANSFER,
    COALESCE(b.OTHER_VAR1, a.OTHER_VAR1) AS OTHER_VAR1,
    COALESCE(b.OTHER_VAR2, a.OTHER_VAR2) AS OTHER_VAR2
FROM NEW_LINES2 a
FULL OUTER JOIN NEW_BASE_XFER_IN b
    ON a.POSN = b.POSN AND a.MONTH = b.MONTH
""")
# END OF SUBCHUNK 19
# BEGIN SUBCHUNK 20
# SUBCHUNK 20: Aggregates base transfer out, then merges into NEW_LINES3 for enriched base transfer columns.
spark.sql("""
    CREATE OR REPLACE TEMP VIEW BASE_XFER_OUT AS
    SELECT
        TRAIN_START_MONTH_1 AS MONTH,
        POSN,
        SUM(PBT) AS BASE_TRANSFER_OUT
    FROM BASE_XFER_INFO
    GROUP BY TRAIN_START_MONTH_1, POSN
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW NEW_BASE_XFER_OUT AS
    SELECT
        a.MONTH,
        a.ACFT,
        a.SEAT,
        a.DOM,
        a.AVAIL,
        a.FLX_FLYING_LINE,
        a.TOT_AVAIL,
        a.RETIRE,
        a.BASE_TRANSFER_IN,
        b.BASE_TRANSFER_OUT,
        a.BASE_TRANSFER_IN + b.BASE_TRANSFER_OUT AS BASE_TRANSFER,
        a.TRANSFER_OUT,
        a.TRANSFER_IN,
        a.NH_ACTIVATED,
        a.FLX_TO_LINE,
        a.NET_CHANGE,
        a.POSN,
        a.TYPE
    FROM NEW_LINES3 a
    INNER JOIN BASE_XFER_OUT b
        ON a.MONTH = b.MONTH AND a.POSN = b.POSN
    ORDER BY a.POSN, a.MONTH
""")
# END OF SUBCHUNK 20
# BEGIN SUBCHUNK 21
# 
# This step updates WORK.NEW_LINES3 with WORK.NEW_BASE_XFER_OUT by POSN and MONTH,
# producing the updated WORK.NEW_LINES4. In Spark SQL, this is a prioritized "MERGE" (NEW_BASE_XFER_OUT supersedes),
# emulating SAS UPDATE logic.

spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_LINES4 AS
SELECT
    COALESCE(b.POSN, a.POSN) AS POSN,
    COALESCE(b.MONTH, a.MONTH) AS MONTH,
    -- For all columns, prefer b (NEW_BASE_XFER_OUT) if not null, else a (NEW_LINES3)
    COALESCE(b.BASE_TRANSFER, a.BASE_TRANSFER) AS BASE_TRANSFER,
    COALESCE(b.BASE_TRANSFER_IN, a.BASE_TRANSFER_IN) AS BASE_TRANSFER_IN,
    COALESCE(b.BASE_TRANSFER_OUT, a.BASE_TRANSFER_OUT) AS BASE_TRANSFER_OUT,
    COALESCE(b.OTHER_COLUMN_1, a.OTHER_COLUMN_1) AS OTHER_COLUMN_1, -- Replace with all relevant columns
    COALESCE(b.OTHER_COLUMN_2, a.OTHER_COLUMN_2) AS OTHER_COLUMN_2
FROM NEW_LINES3 a
FULL OUTER JOIN NEW_BASE_XFER_OUT b
    ON a.POSN = b.POSN AND a.MONTH = b.MONTH
""")
#
# END OF SUBCHUNK 21
# BEGIN SUBCHUNK 22
# Create WORK.XFERS by selecting transfers with different first 3 chars in TRAIN_POS_1 vs CUR_POS_1 and POSN = CUR_POS_1
spark.sql("""
CREATE OR REPLACE TEMP VIEW XFERS AS
SELECT
  EMP_NBR,
  TRAIN_START_MONTH_1,
  TRAIN_END_MONTH_1,
  ACFT,
  SEAT,
  DOM,
  POSN,
  TRAIN_POS_1,
  CUR_POS_1,
  -1 AS OUT,
  1 AS IN
FROM CAM_RESULTS
WHERE SUBSTR(TRAIN_POS_1, 1, 3) <> SUBSTR(CUR_POS_1, 1, 3)
  AND POSN = CUR_POS_1
ORDER BY EMP_NBR
""")

# Create WORK.XFER_N_RET by joining XFERS with RETIRE_INFO on EMP_NBR, and filtering where either TRAIN_START_MONTH_1 or TRAIN_END_MONTH_1 > MONTH
spark.sql("""
CREATE OR REPLACE TEMP VIEW XFER_N_RET AS
SELECT
  a.EMP_NBR,
  a.TRAIN_START_MONTH_1,
  a.TRAIN_END_MONTH_1,
  a.ACFT,
  a.SEAT,
  a.DOM,
  a.POSN,
  a.TRAIN_POS_1,
  a.CUR_POS_1,
  a.OUT,
  a.IN,
  b.MONTH
FROM XFERS a
INNER JOIN RETIRE_INFO b
  ON a.EMP_NBR = b.EMP_NBR
WHERE a.TRAIN_START_MONTH_1 > b.MONTH
   OR a.TRAIN_END_MONTH_1 > b.MONTH
ORDER BY a.EMP_NBR
""")
# END OF SUBCHUNK 22
#END OF CHUNK 4#
#BEGIN CHUNK 5#
def transfer_in(obs: int):
    """
    Replicates the SAS macro transfer_in in Databricks. If obs > 0, it captures retired EMP_NBR values
    from work_xfer_n_ret (mirroring SAS SELECT INTO) and builds work_xfer_info excluding those employees;
    otherwise, it includes all employees. It then aggregates transfers-in by TRAIN_END_MONTH_1 and
    TRAIN_POS_1 into work_xfer_in, and finally joins to work_new_lines4 to produce work_new_xfer_in.
    All outputs are materialized as temporary views: work_xfer_info, work_xfer_in, work_new_xfer_in.
    """

    if obs > 0:
        # NOTE: Converted from 'proc sql noprint; select distinct EMP_NBR into :retired ...'
        retired_rows = spark.sql(
            f'''
            SELECT DISTINCT EMP_NBR
            FROM work_xfer_n_ret
            '''
        ).collect()
        retired = [r["EMP_NBR"] for r in retired_rows]

        # Build WORK.XFER_INFO excluding retired employees (anti-semi logic)
        spark.sql(
            f'''
            CREATE OR REPLACE TEMP VIEW work_xfer_info AS
            SELECT
              EMP_NBR,
              TRAIN_START_MONTH_1,
              TRAIN_END_MONTH_1,
              ACFT,
              SEAT,
              DOM,
              POSN,
              TRAIN_POS_1,
              CUR_POS_1,
              `OUT`,
              `IN`
            FROM work_xfers x
            WHERE NOT EXISTS (
              SELECT 1
              FROM work_xfer_n_ret r
              WHERE x.EMP_NBR = r.EMP_NBR
            )
            ORDER BY EMP_NBR
            '''
        )

        spark.sql(
            f'''
            CREATE OR REPLACE TEMP VIEW work_xfer_in AS
            SELECT DISTINCT
              TRAIN_END_MONTH_1 AS MONTH,
              TRAIN_POS_1 AS POSN,
              SUM(`IN`) AS TRANSFER_IN
            FROM work_xfer_info
            GROUP BY TRAIN_END_MONTH_1, TRAIN_POS_1
            '''
        )

        spark.sql(
            f'''
            CREATE OR REPLACE TEMP VIEW work_new_xfer_in AS
            SELECT DISTINCT
              a.MONTH,
              a.ACFT,
              a.SEAT,
              a.DOM,
              a.AVAIL,
              a.FLX_FLYING_LINE,
              a.TOT_AVAIL,
              a.RETIRE,
              a.BASE_TRANSFER_IN,
              a.BASE_TRANSFER_OUT,
              a.BASE_TRANSFER,
              a.TRANSFER_OUT,
              b.TRANSFER_IN,
              a.NH_ACTIVATED,
              a.FLX_TO_LINE,
              a.NET_CHANGE,
              a.POSN,
              a.TYPE
            FROM work_new_lines4 a, work_xfer_in b
            WHERE a.MONTH = b.MONTH AND a.POSN = b.POSN
            ORDER BY POSN, MONTH
            '''
        )

        # NOTE: SAS PROC SQL QUIT has no operational effect in Databricks; no action required.

    else:
        spark.sql(
            f'''
            CREATE OR REPLACE TEMP VIEW work_xfer_info AS
            SELECT
              EMP_NBR,
              TRAIN_START_MONTH_1,
              TRAIN_END_MONTH_1,
              ACFT,
              SEAT,
              DOM,
              POSN,
              TRAIN_POS_1,
              CUR_POS_1,
              `OUT`,
              `IN`
            FROM work_xfers
            ORDER BY EMP_NBR
            '''
        )

        spark.sql(
            f'''
            CREATE OR REPLACE TEMP VIEW work_xfer_in AS
            SELECT DISTINCT
              TRAIN_END_MONTH_1 AS MONTH,
              TRAIN_POS_1 AS POSN,
              SUM(`IN`) AS TRANSFER_IN
            FROM work_xfer_info
            GROUP BY TRAIN_END_MONTH_1, TRAIN_POS_1
            '''
        )

        spark.sql(
            f'''
            CREATE OR REPLACE TEMP VIEW work_new_xfer_in AS
            SELECT DISTINCT
              a.MONTH,
              a.ACFT,
              a.SEAT,
              a.DOM,
              a.AVAIL,
              a.FLX_FLYING_LINE,
              a.TOT_AVAIL,
              a.RETIRE,
              a.BASE_TRANSFER_IN,
              a.BASE_TRANSFER_OUT,
              a.BASE_TRANSFER,
              a.TRANSFER_OUT,
              b.TRANSFER_IN,
              a.NH_ACTIVATED,
              a.FLX_TO_LINE,
              a.NET_CHANGE,
              a.POSN,
              a.TYPE
            FROM work_new_lines4 a, work_xfer_in b
            WHERE a.MONTH = b.MONTH AND a.POSN = b.POSN
            ORDER BY POSN, MONTH
            '''
        )

        # NOTE: SAS PROC SQL QUIT has no operational effect in Databricks; no action required.
#END OF CHUNK 5#
#BEGIN CHUNK 6#
# BEGIN SUBCHUNK 1
def transfer_in():
    """
    Executes logic corresponding to the SAS %transfer_in macro.
    The macro's internal steps should be refactored as SQL-first transformations.
    If the macro is defined elsewhere, ensure its equivalent Python function is imported or implemented.
    """
    # SKIPPED: Macro body not provided; please supply definition for full conversion.
# END OF SUBCHUNK 1
# BEGIN SUBCHUNK 2
# Update NEW_LINES5 by merging NEW_LINES4 and NEW_XFER_IN on POSN and MONTH, replicating SAS UPDATE logic.
spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_LINES5 AS
WITH
base AS (
  SELECT * FROM NEW_LINES4
),
update AS (
  SELECT * FROM NEW_XFER_IN
),
merged AS (
  SELECT
    COALESCE(u.POSN, b.POSN) AS POSN,
    COALESCE(u.MONTH, b.MONTH) AS MONTH,
    -- Prefer update values if present, else base values
    COALESCE(u.col1, b.col1) AS col1,
    COALESCE(u.col2, b.col2) AS col2,
    COALESCE(u.col3, b.col3) AS col3,
    -- Add all other columns similarly, matching SAS update semantics
    -- If NEW_XFER_IN has columns not in NEW_LINES4, include them; otherwise default to base
    -- Replace col1, col2, col3 with actual column names present in both tables
    *
  FROM base b
  LEFT JOIN update u
    ON b.POSN = u.POSN AND b.MONTH = u.MONTH
)
SELECT * FROM merged
""")
# END OF SUBCHUNK 2
# BEGIN SUBCHUNK 3
# SUBCHUNK 3: Aggregate transfer out info and join to NEW_LINES5 for output as NEW_XFER_OUT

spark.sql("""
    CREATE OR REPLACE TEMP VIEW XFER_OUT AS
    SELECT
        TRAIN_START_MONTH_1 AS MONTH,
        CUR_POS_1 AS POSN,
        SUM(OUT) AS TRANSFER_OUT
    FROM XFER_INFO
    GROUP BY TRAIN_START_MONTH_1, CUR_POS_1
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW NEW_XFER_OUT AS
    SELECT DISTINCT
        a.MONTH,
        a.ACFT,
        a.SEAT,
        a.DOM,
        a.AVAIL,
        a.FLX_FLYING_LINE,
        a.TOT_AVAIL,
        a.RETIRE,
        a.BASE_TRANSFER_IN,
        a.BASE_TRANSFER_OUT,
        a.BASE_TRANSFER,
        b.TRANSFER_OUT,
        a.TRANSFER_IN,
        a.NH_ACTIVATED,
        a.FLX_TO_LINE,
        a.NET_CHANGE,
        a.POSN,
        a.TYPE
    FROM NEW_LINES5 a
    JOIN XFER_OUT b
      ON a.MONTH = b.MONTH AND a.POSN = b.POSN
    ORDER BY a.POSN, a.MONTH
""")
# END OF SUBCHUNK 3
# BEGIN SUBCHUNK 4
# SUBCHUNK 4: Update NEW_LINES6 by merging NEW_LINES5 and NEW_XFER_OUT on POSN and MONTH
spark.sql("""
CREATE OR REPLACE TEMP VIEW NEW_LINES6 AS
SELECT
    COALESCE(nxo.POSN, nl5.POSN) AS POSN,
    COALESCE(nxo.MONTH, nl5.MONTH) AS MONTH,
    -- Columns from NEW_XFER_OUT take precedence; fallback to NEW_LINES5 if missing
    COALESCE(nxo.COL1, nl5.COL1) AS COL1,
    COALESCE(nxo.COL2, nl5.COL2) AS COL2,
    COALESCE(nxo.COL3, nl5.COL3) AS COL3,
    COALESCE(nxo.COL4, nl5.COL4) AS COL4,
    COALESCE(nxo.COL5, nl5.COL5) AS COL5,
    COALESCE(nxo.COL6, nl5.COL6) AS COL6,
    COALESCE(nxo.COL7, nl5.COL7) AS COL7,
    COALESCE(nxo.COL8, nl5.COL8) AS COL8,
    COALESCE(nxo.COL9, nl5.COL9) AS COL9,
    COALESCE(nxo.COL10, nl5.COL10) AS COL10
FROM NEW_LINES5 nl5
FULL OUTER JOIN NEW_XFER_OUT nxo
  ON nl5.POSN = nxo.POSN AND nl5.MONTH = nxo.MONTH
""")
# END OF SUBCHUNK 4
# BEGIN SUBCHUNK 5
# This block creates NEWHIRE_INFO by filtering CAM_RESULTS for new hires, summarizes activations in NEWHIRE, and joins activations to NEW_LINES6 to produce NEW_NEWHIRE.

spark.sql("""
    CREATE OR REPLACE TEMP VIEW NEWHIRE_INFO AS
    SELECT
        EMP_NBR,
        TRAIN_START_MONTH_1,
        CAST(TRAIN_END_MONTH_1 AS DATE) AS MONTH,
        ACFT,
        SEAT,
        DOM,
        POSN,
        TRAIN_POS_1,
        CUR_POS_1,
        PRIM_CLASS_CD
    FROM CAM_RESULTS
    WHERE PRIM_CLASS_CD = 'STU'
    ORDER BY MONTH, EMP_NBR
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW NEWHIRE AS
    SELECT DISTINCT
        POSN,
        MONTH,
        ACFT,
        SEAT,
        DOM,
        COUNT(EMP_NBR) AS NH_ACTIVATED
    FROM NEWHIRE_INFO
    GROUP BY POSN, MONTH, ACFT, SEAT, DOM
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW NEW_NEWHIRE AS
    SELECT DISTINCT
        a.MONTH,
        a.ACFT,
        a.SEAT,
        a.DOM,
        a.AVAIL,
        a.FLX_FLYING_LINE,
        a.TOT_AVAIL,
        a.RETIRE,
        a.BASE_TRANSFER_IN,
        a.BASE_TRANSFER_OUT,
        a.BASE_TRANSFER,
        a.TRANSFER_OUT,
        a.TRANSFER_IN,
        b.NH_ACTIVATED,
        a.FLX_TO_LINE,
        a.NET_CHANGE,
        a.POSN,
        a.TYPE
    FROM NEW_LINES6 a
    JOIN NEWHIRE b
        ON a.MONTH = b.MONTH AND a.POSN = b.POSN
    ORDER BY POSN, MONTH
""")
# END OF SUBCHUNK 5
# BEGIN SUBCHUNK 6
# SUBCHUNK 6: Update WORK.NEW_LINES7 with NEW_LINES6 and NEW_NEWHIRE using BY POSN MONTH.
# This replicates SAS "UPDATE" logic using a SQL MERGE for in-place update by keys.

spark.sql("""
MERGE INTO WORK.NEW_LINES7 AS tgt
USING (
  SELECT * FROM WORK.NEW_LINES6
  UNION ALL
  SELECT * FROM WORK.NEW_NEWHIRE
) AS src
ON tgt.POSN = src.POSN AND tgt.MONTH = src.MONTH
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
# END OF SUBCHUNK 6
# BEGIN SUBCHUNK 7
# Create CAM_SUMMARY_DATA by selecting UNIQUE_ID, MONTH, and VALUE (renamed to POSITION) from CAM_MONTHS, ordered by MONTH.
spark.sql("""
CREATE OR REPLACE TABLE WORK.CAM_SUMMARY_DATA AS
SELECT
    UNIQUE_ID,
    MONTH,
    VALUE AS POSITION
FROM WORK.CAM_MONTHS
ORDER BY MONTH
""")
# END OF SUBCHUNK 7
# BEGIN SUBCHUNK 8
# SUBCHUNK 8: Frequency count of MONTH and POSITION from CAM_SUMMARY_DATA, output to REPORT_DATA_1 (no percent/cum/col/row stats, no sparse combinations)
spark.sql("""
    CREATE OR REPLACE TABLE WORK.REPORT_DATA_1 AS
    SELECT
        MONTH,
        POSITION,
        COUNT(*) AS COUNT
    FROM WORK.CAM_SUMMARY_DATA
    WHERE MONTH IS NOT NULL AND POSITION IS NOT NULL
    GROUP BY MONTH, POSITION
""")
# END OF SUBCHUNK 8
# BEGIN SUBCHUNK 9
# SUBCHUNK 9: Converts SAS PROC SQL table creation and joins for position availability reporting

spark.sql("""
CREATE OR REPLACE TABLE WORK.REPORT_DATA_2 AS
SELECT
  MONTH,
  POSITION,
  COUNT AS AVAIL
FROM WORK.REPORT_DATA_1
WHERE POSITION IS NOT NULL
ORDER BY MONTH, POSITION
""")

spark.sql("""
CREATE OR REPLACE TABLE WORK.FLX_FLY_LINE AS
SELECT
  MONTH,
  POSN,
  FLX_FLYING_LINE
FROM FO_SRC.FLX_FLYING_LINE
ORDER BY MONTH, POSN
""")

spark.sql("""
CREATE OR REPLACE TABLE WORK.AVAIL AS
SELECT DISTINCT
  a.MONTH,
  a.ACFT,
  a.SEAT,
  a.DOM,
  b.AVAIL,
  a.FLX_FLYING_LINE,
  a.TOT_AVAIL,
  a.RETIRE,
  a.BASE_TRANSFER_IN,
  a.BASE_TRANSFER_OUT,
  a.BASE_TRANSFER,
  a.TRANSFER_OUT,
  a.TRANSFER_IN,
  a.NH_ACTIVATED,
  a.FLX_TO_LINE,
  a.NET_CHANGE,
  a.POSN,
  a.TYPE
FROM WORK.NEW_LINES7 a
JOIN WORK.REPORT_DATA_2 b
  ON a.MONTH = b.MONTH AND a.POSN = b.POSITION
ORDER BY a.POSN, a.MONTH
""")
# END OF SUBCHUNK 9
# BEGIN SUBCHUNK 10
# 
# This block updates WORK.NEW_LINES7 with values from WORK.AVAIL for matching POSN and MONTH, 
# preserving unmatched records, to produce WORK.NEW_LINES8.

spark.sql("""
CREATE OR REPLACE TEMP VIEW WORK.NEW_LINES8 AS
SELECT
    COALESCE(avail.POSN, nl7.POSN) AS POSN,
    COALESCE(avail.MONTH, nl7.MONTH) AS MONTH,
    -- Prefer AVAIL columns if present, otherwise NEW_LINES7
    COALESCE(avail.AVAIL, nl7.AVAIL) AS AVAIL,
    -- Select all other columns from AVAIL if present, otherwise from NEW_LINES7
    -- Here you must enumerate all columns that might be updated, otherwise fallback to AVAIL.*
    -- If schemas are aligned and AVAIL is a superset, prefer AVAIL.*, else enumerate
    -- The below assumes AVAIL has all columns in NEW_LINES7 (update-in-place semantics)
    -- Replace this column list with explicit ones if your schema differs
    COALESCE(avail.COL1, nl7.COL1) AS COL1,
    COALESCE(avail.COL2, nl7.COL2) AS COL2
    -- Add additional columns as needed
FROM WORK.NEW_LINES7 nl7
FULL OUTER JOIN WORK.AVAIL avail
  ON nl7.POSN = avail.POSN
 AND nl7.MONTH = avail.MONTH
""")
#
# END OF SUBCHUNK 10
# BEGIN SUBCHUNK 11
# Subchunk 11: Create WORK.FLX_FLY by joining NEW_LINES8 and FLX_FLY_LINE on MONTH and POSN, adding FLX_FLYING_LINE and calculated TOT_AVAIL, ordered by POSN and MONTH.
spark.sql("""
CREATE OR REPLACE TEMP VIEW FLX_FLY AS
SELECT DISTINCT
  a.MONTH,
  a.ACFT,
  a.SEAT,
  a.DOM,
  a.AVAIL,
  b.FLX_FLYING_LINE,
  (b.FLX_FLYING_LINE + a.AVAIL) AS TOT_AVAIL,
  a.RETIRE,
  a.BASE_TRANSFER_IN,
  a.BASE_TRANSFER_OUT,
  a.BASE_TRANSFER,
  a.TRANSFER_OUT,
  a.TRANSFER_IN,
  a.NH_ACTIVATED,
  a.FLX_TO_LINE,
  a.NET_CHANGE,
  a.POSN,
  a.TYPE
FROM NEW_LINES8 a
JOIN FLX_FLY_LINE b
  ON a.MONTH = b.MONTH AND a.POSN = b.POSN
ORDER BY a.POSN, a.MONTH
""")
# END OF SUBCHUNK 11
# BEGIN SUBCHUNK 12
# This chunk updates WORK.NEW_LINES8 with WORK.FLX_FLY by POSN and MONTH, mimicking SAS DATA step UPDATE logic.
spark.sql("""
CREATE OR REPLACE TEMP VIEW WORK.NEW_LINES9 AS
WITH base AS (
  SELECT
    COALESCE(b.POSN, a.POSN) AS POSN,
    COALESCE(b.MONTH, a.MONTH) AS MONTH,
    -- Bring all columns from NEW_LINES8 (a), update with FLX_FLY (b) where present
    COALESCE(b.FLX_FLYING_LINE, a.FLX_FLYING_LINE) AS FLX_FLYING_LINE,
    COALESCE(b.TOT_AVAIL, a.TOT_AVAIL) AS TOT_AVAIL,
    COALESCE(b.AVAIL, a.AVAIL) AS AVAIL,
    COALESCE(b.<other_columns>, a.<other_columns>) AS <other_columns>
  FROM WORK.NEW_LINES8 a
  FULL OUTER JOIN WORK.FLX_FLY b
    ON a.POSN = b.POSN AND a.MONTH = b.MONTH
)
SELECT * FROM base
""")
# Note: Replace <other_columns> with the actual list of non-key columns present in NEW_LINES8/FLX_FLY.
# END OF SUBCHUNK 12
# BEGIN SUBCHUNK 13
# Create CREW_FORECAST table with calculated TOT_AVAIL and NET_CHANGE, ordered by POSN and MONTH

spark.sql("""
CREATE OR REPLACE TABLE WORK.CREW_FORECAST AS
SELECT DISTINCT
    a.MONTH,
    a.ACFT,
    a.SEAT,
    a.DOM,
    a.AVAIL,
    a.FLX_FLYING_LINE,
    CASE WHEN a.FLX_FLYING_LINE = 0 THEN a.AVAIL ELSE a.TOT_AVAIL END AS TOT_AVAIL,
    a.RETIRE,
    a.BASE_TRANSFER_IN,
    a.BASE_TRANSFER_OUT,
    a.BASE_TRANSFER,
    a.TRANSFER_OUT,
    a.TRANSFER_IN,
    a.NH_ACTIVATED,
    a.FLX_TO_LINE,
    (a.RETIRE + a.BASE_TRANSFER + a.TRANSFER_OUT + a.TRANSFER_IN + a.NH_ACTIVATED + a.FLX_TO_LINE) AS NET_CHANGE,
    a.POSN,
    a.TYPE
FROM WORK.NEW_LINES9 a
ORDER BY a.POSN, a.MONTH
""")
# END OF SUBCHUNK 13
# BEGIN SUBCHUNK 14
# SUBCHUNK 14: Append CREW_FORECAST rows to FORECAST; macro end and invocation
# NOTE: PROC APPEND is converted to INSERT INTO for Databricks Delta Lake table append
spark.sql("""
INSERT INTO WORK.FORECAST
SELECT *
FROM WORK.CREW_FORECAST
""")

def combine_scenarios():
    """
    Macro logic: End conditional branches and invoke combine_scenarios.
    This function is a placeholder for the SAS macro, ready for further logic as needed.
    """
    pass

combine_scenarios()
# END OF SUBCHUNK 14
# BEGIN SUBCHUNK 15
# Creates four tables in WORK schema: BASELINE, HISTORY, TARGET_SETTINGS, and CREW_DEMAND.
# Each table selects and renames columns from source tables, with ordering by POSN and MONTH.

spark.sql("""
CREATE OR REPLACE TABLE WORK.BASELINE AS
SELECT
    MONTH,
    ACFT,
    SEAT,
    DOM,
    PROJ_AVAIL AS AVAIL,
    FLX_FLYING_LINE,
    TOT_AVAIL,
    RETIRE,
    BASE_TRANSFER_IN,
    BASE_TRANSFER_OUT,
    BASE_TRANSFER,
    TRANSFER_OUT,
    TRANSFER_IN,
    NH_ACTIVATED,
    FLX_TO_LINE,
    NET_CHANGE,
    POSN,
    'BASELINE' AS TYPE
FROM RESULTS.CAM_BASELINE
ORDER BY POSN, MONTH
""")

spark.sql("""
CREATE OR REPLACE TABLE WORK.HISTORY AS
SELECT
    MONTH,
    ACFT,
    SEAT,
    DOM,
    ACT_AVAIL AS AVAIL,
    FLX_FLYING_LINE,
    TOT_AVAIL,
    RETIRE,
    BASE_TRANSFER_IN,
    BASE_TRANSFER_OUT,
    BASE_TRANSFER,
    TRANSFER_OUT,
    TRANSFER_IN,
    NH_ACTIVATED,
    FLX_TO_LINE,
    NET_CHANGE,
    POSN,
    'HISTORY' AS TYPE
FROM RESULTS.CAM_HISTORY
ORDER BY POSN, MONTH
""")

spark.sql("""
CREATE OR REPLACE TABLE WORK.TARGET_SETTINGS AS
SELECT
    MONTH,
    ACFT,
    SEAT,
    DOM,
    AVAIL,
    FLX_FLYING_LINE,
    TOT_AVAIL,
    RETIRE,
    BASE_TRANSFER_IN,
    BASE_TRANSFER_OUT,
    BASE_TRANSFER,
    TRANSFER_OUT,
    TRANSFER_IN,
    NH_ACTIVATED,
    FLX_TO_LINE,
    NET_CHANGE,
    POSN,
    TYPE
FROM RESULTS.TARGET_SETTINGS
ORDER BY POSN, MONTH
""")

spark.sql("""
CREATE OR REPLACE TABLE WORK.CREW_DEMAND AS
SELECT
    MONTH,
    ACFT,
    SEAT,
    DOM,
    AVAIL,
    FLX_FLYING_LINE,
    TOT_AVAIL,
    RETIRE,
    BASE_TRANSFER_IN,
    BASE_TRANSFER_OUT,
    BASE_TRANSFER,
    TRANSFER_OUT,
    TRANSFER_IN,
    NH_ACTIVATED,
    FLX_TO_LINE,
    NET_CHANGE,
    POSN,
    TYPE
FROM FO_SRC.CREW_DEMAND
ORDER BY POSN, MONTH
""")
# END OF SUBCHUNK 15
#END OF CHUNK 6#
#BEGIN CHUNK 7#
def build_it(cnt: int) -> None:
    """
    Databricks translation of SAS macro build_it.
    If cnt > 0, drops and recreates hive_metastore.pbi_load.crew_availability_forecast by
    UNION-ing, by column name, rows from hive_metastore.work.forecast, history, baseline,
    target_settings, and crew_demand, ordered by type, posn, month. Otherwise, recreates
    from history, baseline, target_settings, and crew_demand only. The drop is guarded for
    idempotency. Set operations use UNION BY NAME to emulate SAS UNION CORR.
    """
    if cnt > 0:
        # SKIPPED: PROC SQL block delimiters (PROC SQL/QUIT) have no direct equivalent; using spark.sql calls below
        spark.sql("DROP TABLE IF EXISTS hive_metastore.pbi_load.crew_availability_forecast")
        spark.sql("""
            CREATE TABLE hive_metastore.pbi_load.crew_availability_forecast AS
            SELECT *
            FROM (
                SELECT * FROM hive_metastore.work.forecast
                UNION BY NAME
                SELECT * FROM hive_metastore.work.history
                UNION BY NAME
                SELECT * FROM hive_metastore.work.baseline
                UNION BY NAME
                SELECT * FROM hive_metastore.work.target_settings
                UNION BY NAME
                SELECT * FROM hive_metastore.work.crew_demand
            ) combined
            ORDER BY type, posn, month
        """)
    else:
        # SKIPPED: PROC SQL block delimiters (PROC SQL/QUIT) have no direct equivalent; using spark.sql calls below
        spark.sql("DROP TABLE IF EXISTS hive_metastore.pbi_load.crew_availability_forecast")
        spark.sql("""
            CREATE TABLE hive_metastore.pbi_load.crew_availability_forecast AS
            SELECT *
            FROM (
                SELECT * FROM hive_metastore.work.history
                UNION BY NAME
                SELECT * FROM hive_metastore.work.baseline
                UNION BY NAME
                SELECT * FROM hive_metastore.work.target_settings
                UNION BY NAME
                SELECT * FROM hive_metastore.work.crew_demand
            ) combined
            ORDER BY type, posn, month
        """)
#END OF CHUNK 7#
#BEGIN CHUNK 8#
def build_it():
    """
    Executes the build_it macro logic.
    This function is a placeholder for the macro execution. 
    The actual transformation logic should be implemented here according to the macro's definition.
    """
    pass  # This function should call the underlying logic as defined in the SAS macro 'build_it'
    
# Call the macro equivalent
build_it()
#END OF CHUNK 8#
#BEGIN CHUNK 9#
def forecast(syserr):
    """
    Implements SAS macro 'forecast' in Python.
    If the system error code (syserr) is zero, this function triggers the insert_table_tmstp utility for the PBI_LOAD library and CREW_FORECAST table. 
    This is typically used to record a timestamp or metadata update after successful processing steps.
    """
    if syserr == 0:
        insert_table_tmstp(_libname="PBI_LOAD", _tablename="CREW_FORECAST")

#END OF CHUNK 9#
#BEGIN CHUNK 10#
# Call the Python function equivalent of the SAS %forecast macro.
forecast()
#END OF CHUNK 10#