import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer_landing
accelerometer_landing_node1779694199286 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-mohan/accelerometer_landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1779694199286")

# Script generated for node customer_trusted
customer_trusted_node1779694198614 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-mohan/customer_trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1779694198614")

# Script generated for node SQL Query
SqlQuery0 = '''
select a.*
from a
inner join c
on a.user = c.email
'''
SQLQuery_node1779694209067 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"a":accelerometer_landing_node1779694199286, "c":customer_trusted_node1779694198614}, transformation_ctx = "SQLQuery_node1779694209067")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1779694209067, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779694161125", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1779694211914 = glueContext.getSink(path="s3://stedi-lake-house-mohan/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1779694211914")
accelerometer_trusted_node1779694211914.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_trusted")
accelerometer_trusted_node1779694211914.setFormat("json")
accelerometer_trusted_node1779694211914.writeFrame(SQLQuery_node1779694209067)
job.commit()
