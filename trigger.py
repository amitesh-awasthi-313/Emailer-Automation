import boto3
import time
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

# --- STEP 0: Load Environment Variables ---
load_dotenv()

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region_name = os.getenv("AWS_REGION")
assume_role_arn = os.getenv("ASSUME_ROLE_ARN")
session_name = os.getenv("ASSUME_ROLE_SESSION")

job1 = os.getenv("GLUE_JOB_1")
job2 = os.getenv("GLUE_JOB_2")

# --- STEP 1: Assume Role ---
sts_client = boto3.client(
    "sts",
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

try:
    response = sts_client.assume_role(
        RoleArn=assume_role_arn,
        RoleSessionName=session_name
    )
    temp_credentials = response["Credentials"]
    print("\u2705 Temporary credentials fetched successfully.")
except ClientError as e:
    print(f"\u274C Failed to assume role: {e.response['Error']['Message']}")
    raise

# --- STEP 2: Use Temporary Credentials for Glue ---
glue_client = boto3.client(
    "glue",
    region_name=region_name,
    aws_access_key_id=temp_credentials["AccessKeyId"],
    aws_secret_access_key=temp_credentials["SecretAccessKey"],
    aws_session_token=temp_credentials["SessionToken"]
)

# --- STEP 3: Job Execution Function ---
def run_glue_job(job_name):
    try:
        start_response = glue_client.start_job_run(JobName=job_name)
        run_id = start_response["JobRunId"]
        print(f"\ud83d\ude80 Started job '{job_name}' | Run ID: {run_id}")
    except ClientError as e:
        print(f"\u274C Failed to start Glue job {job_name}: {e.response['Error']['Message']}")
        raise

    while True:
        job_status = glue_client.get_job_run(JobName=job_name, RunId=run_id)
        state = job_status["JobRun"]["JobRunState"]
        print(f"\u23f3 Job '{job_name}' status: {state}")
        if state in ["SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"]:
            break
        time.sleep(5)

    if state != "SUCCEEDED":
        raise Exception(f"\u274C Glue job '{job_name}' failed with status: {state}")
    print(f"\u2705 Glue job '{job_name}' completed successfully.")

# --- STEP 4: Run Jobs Sequentially ---
run_glue_job(job1)
run_glue_job(job2)
