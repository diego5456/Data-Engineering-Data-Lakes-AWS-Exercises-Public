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
CustomerTrusted_node1737780002977 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1737780002977")

# Script generated for node Accelerator Trusted
AcceleratorTrusted_node1737780829248 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="accelerator_trusted", transformation_ctx="AcceleratorTrusted_node1737780829248")

# Script generated for node Join
Join_node1737780870684 = Join.apply(frame1=AcceleratorTrusted_node1737780829248, frame2=CustomerTrusted_node1737780002977, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1737780870684")

# Script generated for node SQL Query
SqlQuery3790 = '''
select 
    distinct email,
    customername,
    phone,
    birthday,
    serialnumber,
    registrationdate,
    lastupdatedate,
    sharewithresearchasofdate,
    sharewithpublicasofdate,
    sharewithfriendsasofdate
from myDataSource

'''
SQLQuery_node1737780891760 = sparkSqlQuery(glueContext, query = SqlQuery3790, mapping = {"myDataSource":Join_node1737780870684}, transformation_ctx = "SQLQuery_node1737780891760")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1737780891760, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737778233551", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1737781033076 = glueContext.getSink(path="s3://stedi-lake-house-project-dg/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1737781033076")
CustomerCurated_node1737781033076.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="customer_curated")
CustomerCurated_node1737781033076.setFormat("json")
CustomerCurated_node1737781033076.writeFrame(SQLQuery_node1737780891760)
job.commit()