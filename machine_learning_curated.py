import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer trusted
accelerometertrusted_node1701295673432 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_sample",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometertrusted_node1701295673432",
)

# Script generated for node step trainer trusted
steptrainertrusted_node1701295651099 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_sample",
    table_name="step_trainer_trusted",
    transformation_ctx="steptrainertrusted_node1701295651099",
)

# Script generated for node SQL Query
SqlQuery943 = """
select s.*, a.* 
from s
join a
on a.timestamp = s.sensorreadingtime

"""
SQLQuery_node1701295705079 = sparkSqlQuery(
    glueContext,
    query=SqlQuery943,
    mapping={
        "s": steptrainertrusted_node1701295651099,
        "a": accelerometertrusted_node1701295673432,
    },
    transformation_ctx="SQLQuery_node1701295705079",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node1701295889928 = glueContext.getSink(
    path="s3://shengnan-zhao-lake-house/project/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node1701295889928",
)
machine_learning_curated_node1701295889928.setCatalogInfo(
    catalogDatabase="stedi_sample", catalogTableName="machine_learning_trusted"
)
machine_learning_curated_node1701295889928.setFormat("json")
machine_learning_curated_node1701295889928.writeFrame(SQLQuery_node1701295705079)
job.commit()
