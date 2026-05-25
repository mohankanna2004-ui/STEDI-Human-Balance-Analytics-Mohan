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

# Script generated for node step-trainer landing to trusted
steptrainerlandingtotrusted_node1779541506466 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-sample/step_trainer/landing"], "recurse": True}, transformation_ctx="steptrainerlandingtotrusted_node1779541506466")

# Script generated for node step_trainer landing to trusted
step_trainerlandingtotrusted_node1779541513369 = ApplyMapping.apply(frame=steptrainerlandingtotrusted_node1779541506466, mappings=[("sensorReadingTime", "long", "sensorReadingTime", "long"), ("serialNumber", "string", "serialNumber", "string"), ("distanceFromObject", "int", "distanceFromObject", "int")], transformation_ctx="step_trainerlandingtotrusted_node1779541513369")

# Script generated for node trusted step_trainer landing 
EvaluateDataQuality().process_rows(frame=step_trainerlandingtotrusted_node1779541513369, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779537994871", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
trustedstep_trainerlanding_node1779541517282 = glueContext.write_dynamic_frame.from_options(frame=step_trainerlandingtotrusted_node1779541513369, connection_type="s3", format="json", connection_options={"path": "s3://stedi-human-sample/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="trustedstep_trainerlanding_node1779541517282")

job.commit()