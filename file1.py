### GCP COMPLIANCE BLOCK ####

import sys
import os
from google.cloud import bigquery
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.utils import AnalysisException

client = bigquery.Client()

########################### install wheel library manually ###########################################
# !gsutil cp gs://fedex_sas_to_pyspark/library/express_macrolib-0.1.0-py3-none-any.whl .
# # Step 2: Install the wheel file
# !pip install express_macrolib-0.1.0-py3-none-any.whl
# !pip install paramiko
#########################################################################################################
project_id = "123"
notebook_name = "p209d03"
gcs_base_path = f"gs://dna-marketing-datasets/{notebook_name}"

client.create_dataset(f"{project_id}.{notebook_name}", exists_ok=True)
print(f"Dataset {notebook_name} is ready.")


def lowcase(df):
    """Convert all column names to lower case"""
    return df.toDF(*[c.lower() for c in df.columns])

# Safely Read Parquet files
try:
    orion__order_fact = lowcase(spark.read.parquet(f"{gcs_base_path}/orion/order_fact/"))
except AnalysisException:
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_retail_price", DoubleType(), True)
    ])
    orion__order_fact = spark.createDataFrame([], schema)
orion__order_fact.createOrReplaceTempView("orion__order_fact")

try:
    orion__customer = lowcase(spark.read.parquet(f"{gcs_base_path}/orion/customer/"))
except AnalysisException:
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True)
    ])
    orion__customer = spark.createDataFrame([], schema)
orion__customer.createOrReplaceTempView("orion__customer")

try:
    orion__product_dim = lowcase(spark.read.parquet(f"{gcs_base_path}/orion/product_dim/"))
except AnalysisException:
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("supplier_country", StringType(), True),
        StructField("supplier_id", StringType(), True)
    ])
    orion__product_dim = spark.createDataFrame([], schema)
orion__product_dim.createOrReplaceTempView("orion__product_dim")


# ---------------------------------------------------------------------------
# Step 1
# SAS: merge orion.customer(in=cust) work.order_fact(in=order); by Customer_ID;
#      if cust=1 and order=1;
#      keep Customer_ID Customer_Name Quantity Total_Retail_Price Product_ID;
# PySpark: Inner Join on customer_id
# ---------------------------------------------------------------------------
print("\n--- Step 1 ---")
order_fact_2007 = orion__order_fact.filter(F.year(col("order_date")) == 2007)

# An inner join natively fulfills the `if cust=1 and order=1` requirement
custord = orion__customer.join(order_fact_2007, on="customer_id", how="inner")

# Keep specific columns
keep_cols_1 = ["customer_id", "customer_name", "quantity", "total_retail_price", "product_id"]
custord = custord.select(*[c for c in keep_cols_1 if c in custord.columns])
custord.createOrReplaceTempView("custord")


# ---------------------------------------------------------------------------
# Step 2
# SAS: merge CustOrd(in=ord) orion.product_dim(in=prod); by Product_ID;
#      if ord=1 and prod=1;
#      Supplier=catx(' ',Supplier_Country,Supplier_ID);
#      keep Customer_Name Quantity Total_Retail_Price Product_Name Supplier;
# PySpark: Inner Join on product_id, concat_ws for catx
# ---------------------------------------------------------------------------
print("\n--- Step 2 ---")
# Inner join natively fulfills `if ord=1 and prod=1`
custordprod = custord.join(orion__product_dim, on="product_id", how="inner")

# SAS `catx(' ', a, b)` concatenates with a space and ignores missing/null values.
# PySpark `concat_ws(" ", ...)` behaves identically, skipping nulls natively.
custordprod = custordprod.withColumn(
    "supplier",
    F.concat_ws(" ", 
                F.trim(col("supplier_country")), 
                F.trim(col("supplier_id").cast("string"))
    )
)

# Keep specific columns
keep_cols_2 = ["customer_name", "quantity", "total_retail_price", "product_name", "supplier"]
custordprod = custordprod.select(*[c for c in keep_cols_2 if c in custordprod.columns])
custordprod.createOrReplaceTempView("custordprod")

# ---------------------------------------------------------------------------
# SAS: proc print data=CustOrdProd(obs=15) noobs;
# ---------------------------------------------------------------------------
print("CustOrdProd data (First 15):")
custordprod.limit(15).show(truncate=False)


# -------------------------------------------------------------
# DYNAMIC BIGQUERY WRITER HELPER
# Prevents writing empty schemas and checks existence safely
# -------------------------------------------------------------
def safe_write_bq(df_name, table_name_override=None):
    if df_name in globals():
        df_obj = globals()[df_name]
        
        tbl = table_name_override if table_name_override else df_name
        bq_output_table = f"{project_id}.{notebook_name}.{tbl}"
        
        print(f"Dropping table if exists: {bq_output_table}")
        spark.sql(f"DROP TABLE IF EXISTS `{bq_output_table}`")
        
        if df_obj is not None and len(df_obj.columns) > 0:
            print(f"Writing data to BigQuery table: {bq_output_table}")
            try:
                df_obj.write \
                    .format("bigquery") \
                    .option("table", bq_output_table) \
                    .option("temporaryGcsBucket", gcs_base_path.replace("gs://", "").split("/")[0]) \
                    .mode("overwrite") \
                    .save()
                print(f"BigQuery load completed successfully for '{df_name}'.")
            except (AnalysisException, OSError, RuntimeError, ValueError) as e:
                print(f"Error writing '{df_name}': {e}")
        else:
            print(f"Skipping BigQuery write for '{df_name}' (empty schema or zero columns).")
    else:
        print(f"Skipping BigQuery write for '{df_name}' (variable not defined).")

# Execute Safe Writes
safe_write_bq("custord")
safe_write_bq("custordprod")

print("Dry Run completed successfully.")
