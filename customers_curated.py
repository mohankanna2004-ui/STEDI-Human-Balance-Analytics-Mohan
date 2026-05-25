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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1701283264491 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_sample",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1701283264491",
)

# Script generated for node customer_trusted
customer_trusted_node1701283247414 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_sample",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1701283247414",
)

# Script generated for node Join
Join_node1701283306330 = Join.apply(
    frame1=accelerometer_trusted_node1701283264491,
    frame2=customer_trusted_node1701283247414,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1701283306330",
)

# Script generated for node acceleometer filter
SqlQuery897 = """
select distinct customername, email, phone, birthday, serialnumber, 
registrationdate,lastupdatedate, 
sharewithresearchasofdate, sharewithpublicasofdate, sharewithfriendsasofdate
from myDataSource
where x is not null and y is not null and z is not null
"""
acceleometerfilter_node1701291380210 = sparkSqlQuery(
    glueContext,
    query=SqlQuery897,
    mapping={"myDataSource": Join_node1701283306330},
    transformation_ctx="acceleometerfilter_node1701291380210",
)

# Script generated for node customer_curated
customer_curated_node1701283546498 = glueContext.getSink(
    path="s3://shengnan-zhao-lake-house/project/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node1701283546498",
)
customer_curated_node1701283546498.setCatalogInfo(
    catalogDatabase="stedi_sample", catalogTableName="customer_curated"
)
customer_curated_node1701283546498.setFormat("json")
customer_curated_node1701283546498.writeFrame(acceleometerfilter_node1701291380210)
job.commit()
