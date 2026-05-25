import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Amazon S3
AmazonS3_node1779538077147 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-sample/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1779538077147")

# Script generated for node privacyfilter
privacyfilter_node1779538126131 = ApplyMapping.apply(frame=AmazonS3_node1779538077147, mappings=[("customerName", "string", "customerName", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthDay", "string", "birthDay", "string"), ("serialNumber", "string", "serialNumber", "string"), ("registrationDate", "long", "registrationDate", "long"), ("lastUpdateDate", "long", "lastUpdateDate", "long"), ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "long"), ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"), ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "long")], transformation_ctx="privacyfilter_node1779538126131")

# Script generated for node trusted customer zone
EvaluateDataQuality().process_rows(frame=privacyfilter_node1779538126131, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779537994871", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
trustedcustomerzone_node1779538130949 = glueContext.write_dynamic_frame.from_options(frame=privacyfilter_node1779538126131, connection_type="s3", format="json", connection_options={"path": "s3://stedi-human-sample/customer/trusted/", "partitionKeys": []}, transformation_ctx="trustedcustomerzone_node1779538130949")

job.commit()