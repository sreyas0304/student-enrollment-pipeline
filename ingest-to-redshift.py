import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Transformed Data Catalog
TransformedDataCatalog_node1732909946093 = glueContext.create_dynamic_frame.from_catalog(database="enrollment_db", table_name="transformed_data", transformation_ctx="TransformedDataCatalog_node1732909946093")

# Script generated for node Drop Duplicates
DropDuplicates_node1732640380645 =  DynamicFrame.fromDF(TransformedDataCatalog_node1732909946093.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1732640380645")

# Script generated for node Change Column Types
ChangeColumnTypes_node1732741512793 = ApplyMapping.apply(frame=DropDuplicates_node1732640380645, mappings=[("e_id", "int", "e_id", "int"), ("s_id", "int", "s_id", "int"), ("c_id", "int", "c_id", "int"), ("instructor_id", "int", "instructor_id", "int"), ("enrollment_date", "string", "enrollment_date", "string"), ("marks_scored", "int", "marks_scored", "int"), ("grade", "string", "grade", "string"), ("attendance_percentage", "float", "attendance_percentage", "float"), ("payment_status", "string", "payment_status", "string"), ("tuition_amount", "float", "tuition_amount", "float"), ("course_credits", "int", "course_credits", "int"), ("course_major", "string", "course_major", "string"), ("ins_rating", "float", "ins_rating", "float")], transformation_ctx="ChangeColumnTypes_node1732741512793")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1732397936857_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
    IsComplete "enrollment_date",
    IsPrimaryKey "e_id",
    ColumnValues "marks_scored" between 0 and 100,
    ColumnValues "attendance_percentage" between 0 and 100,
    (ColumnValues "ins_rating" > 0) and (ColumnValues "ins_rating" < 5)
    ]
"""

EvaluateDataQuality_node1732397936857 = EvaluateDataQuality().process_rows(frame=ChangeColumnTypes_node1732741512793, ruleset=EvaluateDataQuality_node1732397936857_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1732397936857", "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node originalData
originalData_node1732398290965 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1732397936857, key="originalData", transformation_ctx="originalData_node1732398290965")

# Script generated for node ruleOutcomes
ruleOutcomes_node1732403053373 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1732397936857, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1732403053373")

# Script generated for node Change Schema
ChangeSchema_node1732577798318 = ApplyMapping.apply(frame=originalData_node1732398290965, mappings=[("e_id", "int", "e_id", "int"), ("s_id", "int", "s_id", "int"), ("c_id", "int", "c_id", "int"), ("instructor_id", "int", "instructor_id", "int"), ("enrollment_date", "string", "enrollment_date", "string"), ("marks_scored", "int", "marks_scored", "int"), ("grade", "string", "grade", "string"), ("attendance_percentage", "float", "attendance_percentage", "float"), ("payment_status", "string", "payment_status", "string"), ("tuition_amount", "float", "tuition_amount", "float"), ("course_credits", "int", "course_credits", "int"), ("course_major", "string", "course_major", "string"), ("ins_rating", "float", "ins_rating", "float")], transformation_ctx="ChangeSchema_node1732577798318")

# Script generated for node Amazon S3
AmazonS3_node1732403069793 = glueContext.write_dynamic_frame.from_options(frame=ruleOutcomes_node1732403053373, connection_type="s3", format="glueparquet", connection_options={"path": "s3://student-enrollment-data/data-quality-results/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1732403069793")

# Script generated for node Amazon Redshift
AmazonRedshift_node1732398323815 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1732577798318, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO public.enrollment_fact_new USING public.enrollment_fact_new_temp_9ronoi ON enrollment_fact_new.e_id = enrollment_fact_new_temp_9ronoi.e_id WHEN MATCHED THEN UPDATE SET e_id = enrollment_fact_new_temp_9ronoi.e_id, s_id = enrollment_fact_new_temp_9ronoi.s_id, c_id = enrollment_fact_new_temp_9ronoi.c_id, instructor_id = enrollment_fact_new_temp_9ronoi.instructor_id, enrollment_date = enrollment_fact_new_temp_9ronoi.enrollment_date, marks_scored = enrollment_fact_new_temp_9ronoi.marks_scored, grade = enrollment_fact_new_temp_9ronoi.grade, attendance_percentage = enrollment_fact_new_temp_9ronoi.attendance_percentage, payment_status = enrollment_fact_new_temp_9ronoi.payment_status, tuition_amount = enrollment_fact_new_temp_9ronoi.tuition_amount, course_credits = enrollment_fact_new_temp_9ronoi.course_credits, course_major = enrollment_fact_new_temp_9ronoi.course_major, ins_rating = enrollment_fact_new_temp_9ronoi.ins_rating WHEN NOT MATCHED THEN INSERT VALUES (enrollment_fact_new_temp_9ronoi.e_id, enrollment_fact_new_temp_9ronoi.s_id, enrollment_fact_new_temp_9ronoi.c_id, enrollment_fact_new_temp_9ronoi.instructor_id, enrollment_fact_new_temp_9ronoi.enrollment_date, enrollment_fact_new_temp_9ronoi.marks_scored, enrollment_fact_new_temp_9ronoi.grade, enrollment_fact_new_temp_9ronoi.attendance_percentage, enrollment_fact_new_temp_9ronoi.payment_status, enrollment_fact_new_temp_9ronoi.tuition_amount, enrollment_fact_new_temp_9ronoi.course_credits, enrollment_fact_new_temp_9ronoi.course_major, enrollment_fact_new_temp_9ronoi.ins_rating); DROP TABLE public.enrollment_fact_new_temp_9ronoi; END;", "redshiftTmpDir": "s3://aws-glue-assets-481665108850-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.enrollment_fact_new_temp_9ronoi", "connectionName": "glue-redshift-connection", "preactions": "CREATE TABLE IF NOT EXISTS public.enrollment_fact_new (e_id INTEGER, s_id INTEGER, c_id INTEGER, instructor_id INTEGER, enrollment_date VARCHAR, marks_scored INTEGER, grade VARCHAR, attendance_percentage REAL, payment_status VARCHAR, tuition_amount REAL, course_credits INTEGER, course_major VARCHAR, ins_rating REAL); DROP TABLE IF EXISTS public.enrollment_fact_new_temp_9ronoi; CREATE TABLE public.enrollment_fact_new_temp_9ronoi (e_id INTEGER, s_id INTEGER, c_id INTEGER, instructor_id INTEGER, enrollment_date VARCHAR, marks_scored INTEGER, grade VARCHAR, attendance_percentage REAL, payment_status VARCHAR, tuition_amount REAL, course_credits INTEGER, course_major VARCHAR, ins_rating REAL);"}, transformation_ctx="AmazonRedshift_node1732398323815")

job.commit()