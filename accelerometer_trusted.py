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

# Script generated for node accelerometer landing
accelerometerlanding_node1779539897830 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-sample/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometerlanding_node1779539897830")

# Script generated for node accelerometer landing to trusted
accelerometerlandingtotrusted_node1779539905168 = ApplyMapping.apply(frame=accelerometerlanding_node1779539897830, mappings=[("user", "string", "user", "string"), ("timestamp", "long", "timestamp", "long"), ("x", "double", "x", "double"), ("y", "double", "y", "double"), ("z", "double", "z", "double")], transformation_ctx="accelerometerlandingtotrusted_node1779539905168")

# Script generated for node trusted acclerometer zone
EvaluateDataQuality().process_rows(frame=accelerometerlandingtotrusted_node1779539905168, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779537994871", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
trustedacclerometerzone_node1779539908476 = glueContext.write_dynamic_frame.from_options(frame=accelerometerlandingtotrusted_node1779539905168, connection_type="s3", format="json", connection_options={"path": "s3://stedi-human-sample/accelerometer/trusted/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="trustedacclerometerzone_node1779539908476")

job.commit()

