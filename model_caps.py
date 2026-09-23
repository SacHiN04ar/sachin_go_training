"""
Module: model_caps
Description: PySpark script to replicate SAS macro logic for downloading 
and uploading model caps, performing access checks, and joining product 
activity flags.
"""

import sys
import os

from google.cloud import bigquery
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# Google Colab / Environment Parameters
project_id = "dna-platorchestrator-dev1-9d5a"
notebook_name = "model_caps"
gcs_base_path = f"gs://dna-marketing-datasets/{notebook_name}"

client = bigquery.Client()
client.create_dataset(f"{project_id}.{notebook_name}", exists_ok=True)
print(f"Dataset {notebook_name} is ready.")


# COMMAND ----------


def lowcase(df):
    """Convert all column names to lower case."""
    df = df.toDF(*[c.lower() for c in df.columns])
    return df


def has_column(df, name: str):
    """Checks whether a dataframe has a given column."""
    return name.casefold() in (c.casefold() for c in df.columns)


def write_to_bq(df, table_suffix):
    """Helper function to write DataFrames to BigQuery and reduce code duplication."""
    if df is None:
        print(f"Skipping BigQuery write for {table_suffix} (DataFrame is None).")
        return

    bq_output_table = f"{project_id}.{notebook_name}.{table_suffix}"
    temp_gcs_bucket = gcs_base_path.replace("gs://", "").split("/")[0]

    print(f"Dropping table if exists: {bq_output_table}")
    spark.sql(f"DROP TABLE IF EXISTS `{bq_output_table}`")
    
    print(f"Writing data to BigQuery table: {bq_output_table}")
    (
        df.write.format("bigquery")
        .option("table", bq_output_table)
        .option("temporaryGcsBucket", temp_gcs_bucket)
        .mode("overwrite")
        .save()
    )
    print(f"BigQuery load completed successfully for {table_suffix}.\n")


# COMMAND ----------

# Global / Macro Variables (Simulated for PySpark)
user_id = "dummy_user"
sysjobid = "12345"
web_flg = "D"
scratch_root = "/mktg/prc76/cmms/upload_files"
http_host = "http://localhost"

# Initialize module-level global DataFrames to satisfy Pylint (W0601)
seg = None
base_infogain = None
prod_act = None

# Remediation Comment: Using dummy data for dry run execution
sasdata__prod_act = spark.createDataFrame(
    [("MOD1", "Y"), ("MOD2", "N")],
    schema=StructType([StructField("mod_id", StringType(), True), StructField("act_flg", StringType(), True)])
)

sasdata__segments = spark.createDataFrame(
    [("SEG1",), ("SEG2",)],
    schema=StructType([StructField("segment", StringType(), True)])
)

sasdata__accesslist = spark.createDataFrame(
    [("dummy_user", "Y", "S")],
    schema=StructType([
        StructField("empid", StringType(), True), 
        StructField("active", StringType(), True), 
        StructField("acc_lvl", StringType(), True)
    ])
)

sasdata__disc_caps = spark.createDataFrame(
    [("MOD1", 10.5, 15.0), ("MOD3", 5.0, 8.0)],
    schema=StructType([
        StructField("mod_id", StringType(), True), 
        StructField("aecap", DoubleType(), True), 
        StructField("mgrcap", DoubleType(), True)
    ])
)

# Apply lowcase
sasdata__prod_act = lowcase(sasdata__prod_act)
sasdata__segments = lowcase(sasdata__segments)
sasdata__accesslist = lowcase(sasdata__accesslist)
sasdata__disc_caps = lowcase(sasdata__disc_caps)

sasdata__prod_act.createOrReplaceTempView('prod_act_src')
sasdata__segments.createOrReplaceTempView('segments_src')
sasdata__accesslist.createOrReplaceTempView('accesslist_src')
sasdata__disc_caps.createOrReplaceTempView('disc_caps_src')


# COMMAND ----------

# GCP Remediation Comment: Implemented HTML TODO regarding HTML Codes below.

def macro_down_models():
    """Generates the download model data, filtering by active products."""
    global seg, base_infogain, prod_act

    seg = sasdata__segments
    seg.createOrReplaceTempView("seg")

    base_infogain_initial = sasdata__disc_caps
    base_infogain_initial.createOrReplaceTempView("base_infogain_temp")

    prod_act = sasdata__prod_act.select("mod_id", "act_flg")
    prod_act.createOrReplaceTempView("prod_act")

    # Scrub with active product list
    base_infogain = spark.sql("""
        SELECT 
            b.mod_id,
            b.aecap,
            b.mgrcap
        FROM base_infogain_temp AS b
        JOIN prod_act AS p ON b.mod_id = p.mod_id
        WHERE p.act_flg = 'Y'
    """)
    base_infogain.createOrReplaceTempView("base_infogain")

    # Count evaluation
    count = base_infogain.count()

    if count == 0:
        print("<h4>No data available </h4>")
    else:
        # Replicating SAS HTML STDOUT outputs
        print('<html>')
        print('<head>')
        print('<title> &nbsp</title>')
        print("<!DOCTYPE HTML PUBLIC '-//W3C//DTD HTML 4.0 Transitional//EN'>")
        print('<link rel="stylesheet" type="text/css" href="db.css">')
        print('<script></script>')
        print('</head>')
        print('<body>')
        print('<br clear=all>')
        print('<p style="font-family:Arial;font-size:8.00pt;color:black;" align=left valign=bottom bgcolor=white>')
        print('<center>')
        
        gfile = f"g{sysjobid}"
        tt = f'<a target=new href="{http_host}/plots/capsdata{gfile}.csv">Download CSV File</a>'
        print(tt)
        
        print('<h4>CSV file columns: Segment code, Model ID, aecap, mgrcap </h4>')
        print('</html>')


def macro_upload_models():
    """Simulates the upload HTML interface generation."""
    worklib = f"{scratch_root}/{user_id}"
    print(f"Worklib initialized at: {worklib}")

    # Replicating SAS HTML STDOUT outputs for upload form
    print('<html>')
    print('<head>')
    print('<script>')
    print('function upload_caps() {')
    print('var sel_file = document.getElementById("data_file");')
    print('var fileName=sel_file.value;')
    print('if (fileName.length<4){ alert("Please Select file to proceed"); return false; }')
    print("var ext=fileName.substring(fileName.lastIndexOf('.') + 1);")
    print('if(ext == "csv" || ext == "CSV") { return true; }')
    print('else { alert("Upload CSV file only"); sel_file.focus(); return false; }')
    print('}')
    print('</script>')
    print('<style> input.btn {font-family:Arial; background-color:#eee; border:1px solid #a5acb2;} ')
    print('input.btnhov {background-color:lightblue; font-weight:bold; color:white;} </style>')
    print('</head>')
    print('<body style="font-family:Arial;font-size:8pt;">')
    print('<br></br><h1></h1>')
    print('<div align=left>')
    print('<form name=upload enctype="multipart/form-data" method="post" action=upload_caps.cgi onSubmit="return upload_caps();">')
    
    tt = f'<input type=hidden name=user_id value={user_id}>'
    print(tt)
    
    print('<table style="font-family:Arial;font-size:8pt;border-collapse:collapse;text-align:center;width:300px;">')
    print('<tr><td nowrap style="background-color:#0198e1;color:white;font-size:8pt;font-weight:bold;padding:3px;">Select File With Discount Caps</td></tr>')
    print('<tr><td nowrap style="background-color:white;"><br><input type=file class=btn name="data_file" id="data_file" style="font-size:11px;width:299px;" onmouseover="this.className=\'btn btnhov\'" onmouseout="this.className=\'btn\'" value="Upload"></td></tr>')
    print('<tr><td nowrap style="background-color:white;"><input type=reset class=btn style="font-size:11px;width:148px;" onmouseover="this.className=\'btn btnhov\'" onmouseout="this.className=\'btn\'" value="Clear">')
    print('<input type=submit class=btn style="font-size:11px;width:148px;" onmouseover="this.className=\'btn btnhov\'" onmouseout="this.className=\'btn\'" value="Upload"><br><br></td></tr>')
    print('<tr><td></td></tr>')
    print('<tr><td nowrap style="font-size:8pt;background-color:red;color:white;text-align:center;padding:3px;width:300px;">Uploaded Files Must Follow The Rules Below</td></tr>')
    print('<tr><td nowrap style="background-color:lightyellow;color:black;text-align:left;padding:3px;width:300px;">')
    print('<ol><li>File Name<ul><li>No Spaces in File Name<li>Use Letters, Numbers, or Underscores Only<li>Extension .csv Only</ul>')
    print('<li>File Structure<ul><li>File should have no headers<li>Download Discount caps file and keep only caps that need to be updated')
    print('<li>Format should be same as the .csv file downloaded<li>Column 1: Model ID <li>Column 2: AE Cap<li>Column 3: MGR Cap</ul></ol>')
    print('</td></tr></table></form></body></html>')


def macro_driver():
    """Main driver macro for routing logic based on user access levels."""
    gfile = f"g{sysjobid}"
    print(f"Generated job ID reference: {gfile}")

    access = "A"
    
    # Retrieve user access level
    user_access_df = sasdata__accesslist.filter(
        (col("empid") == lit(user_id)) & (col("active") == lit("Y"))
    )
    
    if user_access_df.count() > 0:
        access_row = user_access_df.select("acc_lvl").first()
        if access_row and access_row["acc_lvl"]:
            access = str(access_row["acc_lvl"]).strip()
            
    if access == "S":
        if web_flg == "D":
            macro_down_models()
        else:
            macro_upload_models()
    else:
        print('<h3> Admin Access Only </h3>')


# COMMAND ----------

# Execute the main macro logic
macro_driver()


# Write output tables sequentially using helper function
write_to_bq(sasdata__segments, "segments")
write_to_bq(seg, "seg")
write_to_bq(sasdata__accesslist, "accesslist")
write_to_bq(sasdata__disc_caps, "disc_caps")
write_to_bq(base_infogain, "base_infogain")
write_to_bq(prod_act, "prod_act")

print("Dry Run completed successfully.")