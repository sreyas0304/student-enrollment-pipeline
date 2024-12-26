import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
import gs_concat
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node instructors
instructors_node1732731132778 = glueContext.create_dynamic_frame.from_catalog(database="enrollment_db", table_name="instructor_details", transformation_ctx="instructors_node1732731132778")

# Script generated for node Students Data
StudentsData_node1732224814499 = glueContext.create_dynamic_frame.from_catalog(database="enrollment_db", table_name="student_csv", transformation_ctx="StudentsData_node1732224814499")

# Script generated for node enrollment
enrollment_node1732224745167 = glueContext.create_dynamic_frame.from_catalog(database="enrollment_db", table_name="enrollment_details", transformation_ctx="enrollment_node1732224745167")

# Script generated for node Student JSON Data
StudentJSONData_node1733234443906 = glueContext.create_dynamic_frame.from_catalog(database="enrollment_db", table_name="student_json", transformation_ctx="StudentJSONData_node1733234443906")

# Script generated for node course
course_node1732225218870 = glueContext.create_dynamic_frame.from_catalog(database="enrollment_db", table_name="course", transformation_ctx="course_node1732225218870")

# Script generated for node Merge Ins Fname and Lname Columns
MergeInsFnameandLnameColumns_node1733001550630 = instructors_node1732731132778.gs_concat(colName="ins_name", colList=["fname", "lname"], spacer=" ")

# Script generated for node Union
Union_node1733234486138 = sparkUnion(glueContext, unionType = "ALL", mapping = {"source1": StudentJSONData_node1733234443906, "source2": StudentsData_node1732224814499}, transformation_ctx = "Union_node1733234486138")

# Script generated for node Change Schema
ChangeSchema_node1732665579169 = ApplyMapping.apply(frame=course_node1732225218870, mappings=[("course_id", "long", "course_id", "int"), ("start_date", "string", "start_date", "string"), ("end_date", "string", "end_date", "string")], transformation_ctx="ChangeSchema_node1732665579169")

# Script generated for node Change Instructor Schema
ChangeInstructorSchema_node1733001581415 = ApplyMapping.apply(frame=MergeInsFnameandLnameColumns_node1733001550630, mappings=[("instructor_id", "bigint", "instructor_id", "int"), ("department", "string", "ins_department", "string"), ("specialization", "string", "specialization", "string"), ("ins_name", "string", "ins_name", "string")], transformation_ctx="ChangeInstructorSchema_node1733001581415")

# Script generated for node Concatenate Columns
ConcatenateColumns_node1733001047781 = Union_node1733234486138.gs_concat(colName="student_name", colList=["fname", "lname"], spacer=" ")

# Script generated for node Changing column names and schema
Changingcolumnnamesandschema_node1733001114781 = ApplyMapping.apply(frame=ConcatenateColumns_node1733001047781, mappings=[("student_id", "bigint", "student_id", "int"), ("dob", "string", "student_dob", "string"), ("gender", "string", "student_gender", "string"), ("phoneno", "string", "student_phoneno", "string"), ("city", "string", "student_city", "string"), ("student_name", "string", "student_name", "string")], transformation_ctx="Changingcolumnnamesandschema_node1733001114781")

# Script generated for node Join
Join_node1732224865734 = Join.apply(frame1=enrollment_node1732224745167, frame2=Changingcolumnnamesandschema_node1733001114781, keys1=["s_id"], keys2=["student_id"], transformation_ctx="Join_node1732224865734")

# Script generated for node Change Schema
ChangeSchema_node1732224911838 = ApplyMapping.apply(frame=Join_node1732224865734, mappings=[("e_id", "long", "e_id", "long"), ("s_id", "long", "s_id", "long"), ("c_id", "long", "c_id", "long"), ("instructor_id", "long", "instructor_id", "long"), ("enrollment_date", "string", "enrollment_date", "string"), ("marks_scored", "long", "marks_scored", "long"), ("grade", "string", "grade", "string"), ("attendance_percentage", "double", "attendance_percentage", "double"), ("payment_status", "string", "payment_status", "string"), ("tuition_amount", "double", "tuition_amount", "double"), ("student_id", "int", "student_id", "long"), ("student_dob", "string", "student_dob", "string"), ("student_gender", "string", "student_gender", "string"), ("student_phoneno", "string", "student_phoneno", "string"), ("student_city", "string", "student_city", "string"), ("student_name", "string", "student_name", "string")], transformation_ctx="ChangeSchema_node1732224911838")

# Script generated for node Join
Join_node1732225276027 = Join.apply(frame1=ChangeSchema_node1732224911838, frame2=course_node1732225218870, keys1=["c_id"], keys2=["course_id"], transformation_ctx="Join_node1732225276027")

# Script generated for node Change Schema
ChangeSchema_node1732225308652 = ApplyMapping.apply(frame=Join_node1732225276027, mappings=[("e_id", "long", "e_id", "int"), ("s_id", "long", "s_id", "long"), ("c_id", "long", "c_id", "long"), ("instructor_id", "long", "instructor_id", "long"), ("enrollment_date", "string", "enrollment_date", "string"), ("marks_scored", "long", "marks_scored", "int"), ("grade", "string", "grade", "string"), ("attendance_percentage", "double", "attendance_percentage", "double"), ("payment_status", "string", "payment_status", "string"), ("tuition_amount", "double", "tuition_amount", "double"), ("student_id", "long", "student_id", "long"), ("student_dob", "string", "student_dob", "string"), ("student_gender", "string", "student_gender", "string"), ("student_phoneno", "string", "student_phoneno", "string"), ("student_city", "string", "student_city", "string"), ("student_name", "string", "student_name", "string"), ("course_credits", "long", "course_credits", "long"), ("course_major", "string", "course_major", "string")], transformation_ctx="ChangeSchema_node1732225308652")

# Script generated for node instructor join
instructorjoin_node1732731178313 = Join.apply(frame1=ChangeSchema_node1732225308652, frame2=MergeInsFnameandLnameColumns_node1733001550630, keys1=["instructor_id"], keys2=["instructor_id"], transformation_ctx="instructorjoin_node1732731178313")

# Script generated for node Drop Duplicates
DropDuplicates_node1733234640317 =  DynamicFrame.fromDF(instructorjoin_node1732731178313.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1733234640317")

# Script generated for node Final Schema
FinalSchema_node1732731247363 = ApplyMapping.apply(frame=DropDuplicates_node1733234640317, mappings=[("e_id", "int", "e_id", "int"), ("s_id", "long", "s_id", "int"), ("c_id", "long", "c_id", "int"), ("instructor_id", "long", "instructor_id", "int"), ("enrollment_date", "string", "enrollment_date", "string"), ("marks_scored", "int", "marks_scored", "int"), ("grade", "string", "grade", "string"), ("attendance_percentage", "double", "attendance_percentage", "float"), ("payment_status", "string", "payment_status", "string"), ("tuition_amount", "double", "tuition_amount", "float"), ("course_credits", "long", "course_credits", "int"), ("course_major", "string", "course_major", "string"), ("rating", "double", "ins_rating", "float")], transformation_ctx="FinalSchema_node1732731247363")

# Script generated for node Amazon Redshift
AmazonRedshift_node1732665635463 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1732665579169, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO public.course_dim USING public.course_dim_temp_fl2mbp ON course_dim.course_id = course_dim_temp_fl2mbp.course_id WHEN MATCHED THEN UPDATE SET course_id = course_dim_temp_fl2mbp.course_id, start_date = course_dim_temp_fl2mbp.start_date, end_date = course_dim_temp_fl2mbp.end_date WHEN NOT MATCHED THEN INSERT VALUES (course_dim_temp_fl2mbp.course_id, course_dim_temp_fl2mbp.start_date, course_dim_temp_fl2mbp.end_date); DROP TABLE public.course_dim_temp_fl2mbp; END;", "redshiftTmpDir": "s3://aws-glue-assets-481665108850-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.course_dim_temp_fl2mbp", "connectionName": "glue-redshift-connection", "preactions": "CREATE TABLE IF NOT EXISTS public.course_dim (course_id INTEGER, start_date VARCHAR, end_date VARCHAR); DROP TABLE IF EXISTS public.course_dim_temp_fl2mbp; CREATE TABLE public.course_dim_temp_fl2mbp (course_id INTEGER, start_date VARCHAR, end_date VARCHAR);"}, transformation_ctx="AmazonRedshift_node1732665635463")

# Script generated for node Amazon Redshift
AmazonRedshift_node1733001666371 = glueContext.write_dynamic_frame.from_options(frame=ChangeInstructorSchema_node1733001581415, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO public.instructor_dim USING public.instructor_dim_temp_ei0sd4 ON instructor_dim.instructor_id = instructor_dim_temp_ei0sd4.instructor_id WHEN MATCHED THEN UPDATE SET instructor_id = instructor_dim_temp_ei0sd4.instructor_id, ins_department = instructor_dim_temp_ei0sd4.ins_department, specialization = instructor_dim_temp_ei0sd4.specialization, ins_name = instructor_dim_temp_ei0sd4.ins_name WHEN NOT MATCHED THEN INSERT VALUES (instructor_dim_temp_ei0sd4.instructor_id, instructor_dim_temp_ei0sd4.ins_department, instructor_dim_temp_ei0sd4.specialization, instructor_dim_temp_ei0sd4.ins_name); DROP TABLE public.instructor_dim_temp_ei0sd4; END;", "redshiftTmpDir": "s3://aws-glue-assets-481665108850-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.instructor_dim_temp_ei0sd4", "connectionName": "glue-redshift-connection", "preactions": "CREATE TABLE IF NOT EXISTS public.instructor_dim (instructor_id INTEGER, ins_department VARCHAR, specialization VARCHAR, ins_name VARCHAR); DROP TABLE IF EXISTS public.instructor_dim_temp_ei0sd4; CREATE TABLE public.instructor_dim_temp_ei0sd4 (instructor_id INTEGER, ins_department VARCHAR, specialization VARCHAR, ins_name VARCHAR);"}, transformation_ctx="AmazonRedshift_node1733001666371")

# Script generated for node Amazon Redshift
AmazonRedshift_node1733001171198 = glueContext.write_dynamic_frame.from_options(frame=Changingcolumnnamesandschema_node1733001114781, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO public.student_dim_pii USING public.student_dim_pii_temp_433alt ON student_dim_pii.student_id = student_dim_pii_temp_433alt.student_id WHEN MATCHED THEN UPDATE SET student_id = student_dim_pii_temp_433alt.student_id, student_dob = student_dim_pii_temp_433alt.student_dob, student_gender = student_dim_pii_temp_433alt.student_gender, student_phoneno = student_dim_pii_temp_433alt.student_phoneno, student_city = student_dim_pii_temp_433alt.student_city, student_name = student_dim_pii_temp_433alt.student_name WHEN NOT MATCHED THEN INSERT VALUES (student_dim_pii_temp_433alt.student_id, student_dim_pii_temp_433alt.student_dob, student_dim_pii_temp_433alt.student_gender, student_dim_pii_temp_433alt.student_phoneno, student_dim_pii_temp_433alt.student_city, student_dim_pii_temp_433alt.student_name); DROP TABLE public.student_dim_pii_temp_433alt; END;", "redshiftTmpDir": "s3://aws-glue-assets-481665108850-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.student_dim_pii_temp_433alt", "connectionName": "glue-redshift-connection", "preactions": "CREATE TABLE IF NOT EXISTS public.student_dim_pii (student_id INTEGER, student_dob VARCHAR, student_gender VARCHAR, student_phoneno VARCHAR, student_city VARCHAR, student_name VARCHAR); DROP TABLE IF EXISTS public.student_dim_pii_temp_433alt; CREATE TABLE public.student_dim_pii_temp_433alt (student_id INTEGER, student_dob VARCHAR, student_gender VARCHAR, student_phoneno VARCHAR, student_city VARCHAR, student_name VARCHAR);"}, transformation_ctx="AmazonRedshift_node1733001171198")

# Script generated for node Amazon S3
AmazonS3_node1732497648631 = glueContext.write_dynamic_frame.from_options(frame=FinalSchema_node1732731247363, connection_type="s3", format="glueparquet", connection_options={"path": "s3://student-enrollment-data/transformed_data/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1732497648631")

job.commit()