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

# Script generated for node Customer Trusted
CustomerTrusted_node1737779529845 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1737779529845")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1737779574499 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1737779574499")

# Script generated for node Join
Join_node1737779558724 = Join.apply(frame1=CustomerTrusted_node1737779529845, frame2=AccelerometerLanding_node1737779574499, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1737779558724")

# Script generated for node SQL Query
SqlQuery3758 = '''
select user, timestamp,x, y, z from myDataSource

'''
SQLQuery_node1737779626955 = sparkSqlQuery(glueContext, query = SqlQuery3758, mapping = {"myDataSource":Join_node1737779558724}, transformation_ctx = "SQLQuery_node1737779626955")

# Script generated for node Accelerator Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1737779626955, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737778233551", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AcceleratorTrusted_node1737779754864 = glueContext.getSink(path="s3://stedi-lake-house-project-dg/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AcceleratorTrusted_node1737779754864")
AcceleratorTrusted_node1737779754864.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="accelerator_trusted")
AcceleratorTrusted_node1737779754864.setFormat("json")
AcceleratorTrusted_node1737779754864.writeFrame(SQLQuery_node1737779626955)
job.commit()