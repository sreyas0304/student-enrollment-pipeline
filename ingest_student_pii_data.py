import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import gs_concat

def sparkUnion(glueContext, unionType, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql("(select * from source1) UNION " + unionType + " (select * from source2)")
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Students Data
StudentsData_node1732924343271 = glueContext.create_dynamic_frame.from_catalog(database="enrollment_db", table_name="student_csv", transformation_ctx="StudentsData_node1732924343271")

# Script generated for node Student JSON Data
StudentJSONData_node1733234076567 = glueContext.create_dynamic_frame.from_catalog(database="enrollment_db", table_name="student_json", transformation_ctx="StudentJSONData_node1733234076567")

# Script generated for node Union
Union_node1733234106743 = sparkUnion(glueContext, unionType = "ALL", mapping = {"source1": StudentsData_node1732924343271, "source2": StudentJSONData_node1733234076567}, transformation_ctx = "Union_node1733234106743")

# Script generated for node Concatenate Columns
ConcatenateColumns_node1733003365628 = Union_node1733234106743.gs_concat(colName="student_name", colList=["fname", "lname"], spacer=" ")

# Script generated for node Change Schema
ChangeSchema_node1733003526124 = ApplyMapping.apply(frame=ConcatenateColumns_node1733003365628, mappings=[("student_id", "long", "student_id", "int"), ("gender", "string", "student_gender", "string"), ("city", "string", "student_city", "string"), ("student_name", "string", "student_name", "string")], transformation_ctx="ChangeSchema_node1733003526124")

# Script generated for node Amazon Redshift
AmazonRedshift_node1733003770696 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1733003526124, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO public.student_dim USING public.student_dim_temp_lou2c4 ON student_dim.student_id = student_dim_temp_lou2c4.student_id WHEN MATCHED THEN UPDATE SET student_id = student_dim_temp_lou2c4.student_id, student_gender = student_dim_temp_lou2c4.student_gender, student_city = student_dim_temp_lou2c4.student_city, student_name = student_dim_temp_lou2c4.student_name WHEN NOT MATCHED THEN INSERT VALUES (student_dim_temp_lou2c4.student_id, student_dim_temp_lou2c4.student_gender, student_dim_temp_lou2c4.student_city, student_dim_temp_lou2c4.student_name); DROP TABLE public.student_dim_temp_lou2c4; END;", "redshiftTmpDir": "s3://aws-glue-assets-481665108850-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.student_dim_temp_lou2c4", "connectionName": "glue-redshift-connection", "preactions": "CREATE TABLE IF NOT EXISTS public.student_dim (student_id INTEGER, student_gender VARCHAR, student_city VARCHAR, student_name VARCHAR); DROP TABLE IF EXISTS public.student_dim_temp_lou2c4; CREATE TABLE public.student_dim_temp_lou2c4 (student_id INTEGER, student_gender VARCHAR, student_city VARCHAR, student_name VARCHAR);"}, transformation_ctx="AmazonRedshift_node1733003770696")

job.commit()