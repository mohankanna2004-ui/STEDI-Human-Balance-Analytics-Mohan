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

# Script generated for node customer_curated
customer_curated_node1779701632575 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://stedi-lake-house-mohan/customer_curated/"], "recurse": True}, transformation_ctx="customer_curated_node1779701632575")

# Script generated for node step_trainer_landing
step_trainer_landing_node1779701633399 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-mohan/step_trainer_landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1779701633399")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT DISTINCT
    st.sensorreadingtime,
    st.serialnumber,
    st.distancefromobject
FROM st
INNER JOIN c
ON st.serialnumber = c.serialnumber
'''
SQLQuery_node1779701636257 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"st":step_trainer_landing_node1779701633399, "c":customer_curated_node1779701632575}, transformation_ctx = "SQLQuery_node1779701636257")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1779701636257, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779700199933", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1779701639724 = glueContext.getSink(path="s3://stedi-lake-house-mohan/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1779701639724")
step_trainer_trusted_node1779701639724.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1779701639724.setFormat("json")
step_trainer_trusted_node1779701639724.writeFrame(SQLQuery_node1779701636257)
job.commit()
