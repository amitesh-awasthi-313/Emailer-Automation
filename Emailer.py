import sys
import boto3
import pandas as pd
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from dotenv import load_dotenv
import os

# --- Load Environment Variables ---
load_dotenv()

# Current date
today = datetime.today().strftime('%d %b %Y')
# Yesterday's date
yesterday = (datetime.today() - timedelta(days=1)).strftime('%d %b %Y')

# --- Parse job arguments ---
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# --- Initialize Spark & Glue ---
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

# --- Email config from .env ---
my_host = os.getenv("AWS_SES_HOST")
my_port = 587
my_access_key = os.getenv("AWS_SES_USERNAME")
my_secret = os.getenv("AWS_SES_PASSWORD")

# --- Athena + S3 Constants ---
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE")
S3_BUCKET = os.getenv("S3_BUCKET")
REGION_NAME = os.getenv("AWS_REGION")

# --- Query Template Paths ---
QUERY_TEMPLATE_02 = "query_templates/U_platform_wise_users.sql"
QUERY_TEMPLATE_03 = "query_templates/U_BSNL_Registered_users.sql"
QUERY_TEMPLATE_04 = "query_templates/U_Downloads.sql"
QUERY_T EMPLATE_05 = "query_templates/U_BSNL_Registered_users.sql"

# --- Load SQL from S3 ---
s3 = boto3.client('s3')

def get_query_template(bucket, queryTemplatePath):
    response = s3.get_object(Bucket=bucket, Key=queryTemplatePath)
    return response["Body"].read().decode("utf-8").strip()

query02 = get_query_template(S3_BUCKET, QUERY_TEMPLATE_02)
query03 = get_query_template(S3_BUCKET, QUERY_TEMPLATE_03)
query04 = get_query_template(S3_BUCKET, QUERY_TEMPLATE_04)
query05 = get_query_template(S3_BUCKET, QUERY_TEMPLATE_05)

logger.info("Loaded Queries")

# --- Table Headers ---
headers_users = ["Platform", f"{yesterday}", f"{today}", "Growth", "Growth %"]
headers_providers = ["Provider", f"{yesterday}", f"{today}", "Growth", "Growth %"]
headers_athena_downloads = ["Platform", f"{yesterday}", f"{today}", "Growth", "Growth %"]

# --- Execute Queries ---
def run_mysql_query(query):
    dynamic_df = glueContext.create_dynamic_frame.from_options(
        connection_type="mysql",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": "pb_node_ott.users",
            "connectionName": "Aurora connection",
            "sampleQuery": query
        },
        transformation_ctx="MySQL_dynamic_frame"
    )
    return dynamic_df.toDF().toPandas()

def run_sorted_mysql_query(query, sort_by=None, ascending=True):
    df = run_mysql_query(query)
    if sort_by:
        df.sort_values(by=sort_by, ascending=ascending, inplace=True)
    return df

def run_athena_query(query):
    athena = boto3.client('athena', region_name=REGION_NAME)
    output_location = f"s3://{S3_BUCKET}/athena_results/"
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": output_location}
    )
    query_execution_id = response['QueryExecutionId']

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

    if status != 'SUCCEEDED':
        raise Exception("Athena query failed")

    result = athena.get_query_results(QueryExecutionId=query_execution_id)
    headers = [col['VarCharValue'] for col in result['ResultSet']['Rows'][0]['Data']]
    rows = [[col.get('VarCharValue', '') for col in row['Data']] for row in result['ResultSet']['Rows'][1:]]
    return pd.DataFrame(rows, columns=headers)

# --- Convert to HTML ---
def pandas_df_to_html(pandas_df, headers):
    html = "<table border='1' style='border-collapse: collapse;'>"
    html += "<tr>" + "".join([f"<th>{col}</th>" for col in headers]) + "</tr>"
    for _, row in pandas_df.iterrows():
        html += "<tr>" + "".join([f"<td>{val}</td>" for val in row]) + "</tr>"
    html += "</table><br>"
    return html

html2 = pandas_df_to_html(run_sorted_mysql_query(query02 , sort_by="users", ascending=True), headers_users)
html3 = pandas_df_to_html(run_mysql_query(query03), headers_providers)
html4 = pandas_df_to_html(run_athena_query(query04), headers_athena_downloads)
html5 = pandas_df_to_html(run_mysql_query(query05), headers_providers)

final_html = f"<h2>Registered Users - {today}</h2>" + html2
final_html += f"<h2>BBNL & BSNL Users - {today}</h2>" + html3
final_html += f"<h2>Platform-wise Downloads  - {today}</h2>" + html4
final_html += f"<h2>BBNL & BSNL provisioned  Users - {today}</h2>" + html3

logger.info("Final HTML Generated")

# --- Send Email ---
sender_email = "WavesPB Admin <no-reply@wavespb.com>"
receiver_email = "amiteshawasthi7@gmail.com"
subject = f"Daily Report - Platform Wise Stats | {today}"

msg = MIMEMultipart("alternative")
msg['Subject'] = subject
msg['From'] = sender_email
msg['To'] = receiver_email
msg.attach(MIMEText(final_html, "html"))

try:
    with smtplib.SMTP(my_host, my_port) as server:
        server.starttls()
        server.login(my_access_key, my_secret)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        logger.info("Mail sent successfully to " + receiver_email)
except Exception as e:
    logger.error("Mail send failed: " + str(e))

# --- Final Commit ---
job.commit()
