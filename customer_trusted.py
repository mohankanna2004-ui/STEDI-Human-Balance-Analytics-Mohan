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

# Script generated for node customer_landing
customer_landing_node1779690345081 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-mohan/customer_landing/"], "recurse": True}, transformation_ctx="customer_landing_node1779690345081")

# Script generated for node Change Schema
ChangeSchema_node1779758746916 = ApplyMapping.apply(frame=customer_landing_node1779690345081, mappings=[("customerName", "string", "customerName", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthDay", "string", "birthDay", "string"), ("serialNumber", "string", "serialNumber", "string"), ("registrationDate", "bigint", "registrationDate", "bigint"), ("lastUpdateDate", "bigint", "lastUpdateDate", "bigint"), ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "bigint"), ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "bigint"), ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "bigint")], transformation_ctx="ChangeSchema_node1779758746916")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT *
FROM c
WHERE sharewithresearchasofdate IS NOT NULL
'''
SQLQuery_node1779758830792 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"c":ChangeSchema_node1779758746916}, transformation_ctx = "SQLQuery_node1779758830792")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1779758830792, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779758686212", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1779758847559 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1779758830792, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lake-house-mohan/customer_trusted/", "partitionKeys": []}, transformation_ctx="customer_trusted_node1779758847559")

job.commit()
