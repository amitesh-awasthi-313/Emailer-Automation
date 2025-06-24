# 🔁 AWS Glue Job Trigger & Email Automation

A set of Python scripts to automate AWS Glue job execution, extract report data using Athena & MySQL, and send daily summary emails via AWS SES.

---

## 📁 Project Structure

| File | Description |
|------|-------------|
| `trigger.py` | Assumes IAM role using STS and sequentially triggers Glue jobs securely. |
| `emailer.py` | Executes Athena & MySQL queries, builds HTML tables, and emails them via SES. |
| `meta_info_etl.py` | Glue ETL script to extract metadata from Aurora MySQL and write to S3 in Parquet format. |
| `.env.example` | Sample environment variable file for credentials and config (do not upload `.env`). |

---

## ⚙️ Environment Variables (`.env`)

Rename `.env.example` → `.env` and fill in your secrets:

```env
# AWS Credentials
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=ap-south-1

# IAM Role
ASSUME_ROLE_ARN=arn:aws:iam::123456789012:role/your-role
ASSUME_ROLE_SESSION=AssumeRoleManualSession

# Glue Jobs
GLUE_JOB_1=pb_meta_info
GLUE_JOB_2=PB_REGISTERED_USERS_DOWNLOADS_MAILER

# SES Credentials
AWS_SES_HOST=email-smtp.ap-south-1.amazonaws.com
AWS_SES_USERNAME=your-smtp-user
AWS_SES_PASSWORD=your-smtp-pass

# Athena + S3
ATHENA_DATABASE=analyticsdatabase
S3_BUCKET=pb-ott-athena

# Meta Info ETL Specific
S3_META_INFO_PATH=s3://your-bucket/meta_info/
META_INFO_TABLE=Pb_meta_information
MYSQL_CONNECTION_NAME=cms sandbox connection
```

---

## 🚀 Usage

### 1. 🔁 Trigger Glue Jobs
```bash
python trigger.py
```

### 2. 📧 Send Daily Report Email
```bash
python emailer.py --JOB_NAME daily_email_job
```

### 3. 📦 Run Meta Info ETL
```bash
python meta_info_etl.py --JOB_NAME meta_info_etl
```

---

## 🔐 Security Notes
- **DO NOT** commit your `.env` file — it's ignored by `.gitignore`
- Rotate AWS credentials if accidentally exposed
- Use IAM roles and STS wherever possible

---

## 🙌 Author
**Amitesh Awasthi**  
*Data Analytics Lead, Appsquadz Software Pvt. Ltd.*

Connect on [LinkedIn](https://www.linkedin.com/in/amitesh-awasthi-7aa278228/)

---

## 📄 License
MIT License
