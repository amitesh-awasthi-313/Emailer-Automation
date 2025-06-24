import sys
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import current_timestamp
from dotenv import load_dotenv

# --- Load environment variables ---
load_dotenv()

S3_META_INFO_PATH = os.getenv("S3_META_INFO_PATH")
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE")
META_INFO_TABLE = os.getenv("META_INFO_TABLE")
MYSQL_CONNECTION_NAME = os.getenv("MYSQL_CONNECTION_NAME")

# --- Initialize Glue Job ---
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Read from Aurora MySQL ---
MySQL_node = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "sb_node_ott.meta_information",
        "connectionName": MYSQL_CONNECTION_NAME,
    },
    transformation_ctx="MySQL_node"
)

# --- Convert to DataFrame for transformation ---
df = MySQL_node.toDF()

# Add modified_at column with current timestamp
df_with_time = df.withColumn("modified_at", current_timestamp())

# --- Convert back to DynamicFrame ---
final_dyf = DynamicFrame.fromDF(df_with_time, glueContext, "final_dyf")

# --- Write to S3 in Parquet format ---
s3_sink = glueContext.getSink(
    path=S3_META_INFO_PATH,
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="s3_sink"
)
s3_sink.setCatalogInfo(
    catalogDatabase=ATHENA_DATABASE,
    catalogTableName=META_INFO_TABLE
)
s3_sink.setFormat("glueparquet", compression="snappy")
s3_sink.writeFrame(final_dyf)

# --- Final Commit ---
job.commit()
