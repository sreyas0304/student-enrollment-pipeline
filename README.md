# student-enrollment-pipeline
A robust ETL (Extract, Transform, Load) pipeline built on AWS for processing student enrollment data, implementing data quality checks, and maintaining a secure data warehouse.

# Architecture Overview
The pipeline leverages several AWS services to create a scalable and secure data processing workflow:
  - Amazon S3 for data lake storage
  - AWS Glue for ETL processing
  - Amazon Redshift for data warehousing
  - AWS Step Functions for orchestration
  - AWS Lambda for serverless computing
  - Amazon SNS for notifications
  - AWS Lake Formation for data security
# Data Flow

![pipeline architecture drawio](https://github.com/user-attachments/assets/c669a036-f6ca-4a35-8bce-fdf979b779ee)


# Data Ingestion
Raw data lands in S3 bucket's raw_layer
Data includes student information, course details, instructor data, and enrollment records
AWS Glue crawlers automatically detect and catalog new data
# Data Processing
AWS Glue jobs perform multiple transformations:
Schema standardization
Column concatenation (e.g., first name + last name)
Sensitive data detection and redaction
Deduplication
Data quality validation
Data Quality Checks
# Automated validation rules include:
Completeness check for enrollment dates
Primary key validation for enrollment IDs
Range validation for marks (0-100)
Attendance percentage validation (0-100)
Instructor rating validation (0-5)
# Data Loading
Transformed data stored in S3 (Parquet format with Snappy compression)
Data loaded into Redshift star schema:
Fact table: enrollment_fact
Dimension tables: student_dim, course_dim, instructor_dim
# Security Features
Lake Formation
Row-level security
Column-level access control
PII data protection
# Redshift
Role-based access control
Read-only and read-write privileges
Secure data warehouse access


