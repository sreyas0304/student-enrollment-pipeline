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

# Workflow Orchestration
The student enrollment ETL pipeline leverages AWS Step Functions for comprehensive orchestration of the data processing workflow. The orchestration begins by triggering Glue crawlers that extract metadata from raw data sources in S3. Once metadata is cataloged, the workflow executes a series of Glue jobs for data enrichment and PII data processing. After the initial transformations, another crawler runs to catalog the transformed data before loading it into Redshift. The workflow concludes by sending automated notifications through SNS to inform stakeholders about the job completion status, ensuring full visibility of the pipeline's execution.

# Monitoring and Maintenance
The pipeline's health and performance are continuously monitored through multiple touchpoints in the AWS ecosystem. The AWS Glue console provides detailed insights into job execution metrics and status, while data quality results stored in S3 offer visibility into data integrity and validation outcomes. Step Functions execution history enables tracking of the entire workflow, including state transitions and execution times. SNS notifications deliver real-time alerts about job completion status, and Redshift query performance metrics help optimize data warehouse operations.

# Security Considerations
Implement encryption at rest for S3 and Redshift
Use IAM roles with least privilege principle
Secure sensitive data using Lake Formation
Regular audit of access patterns
Monitoring of security logs


