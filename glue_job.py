import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = glueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from S3
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "your_database", table_name = "your_table", transformation_ctx = "datasource0")

# Transform data
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("col1", "string", "col1", "string"), ("col2", "int", "col2", "int")], transformation_ctx = "applymapping1")

# Write data back to S3
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://your-output-bucket/output/"}, format = "json", transformation_ctx = "datasink2")
job.commit()