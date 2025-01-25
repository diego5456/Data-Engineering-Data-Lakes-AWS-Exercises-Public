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

# Script generated for node Customer Landing
CustomerLanding_node1737775135083 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_landing", transformation_ctx="CustomerLanding_node1737775135083")

# Script generated for node SQL Query
SqlQuery4248 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null;

'''
SQLQuery_node1737775161344 = sparkSqlQuery(glueContext, query = SqlQuery4248, mapping = {"myDataSource":CustomerLanding_node1737775135083}, transformation_ctx = "SQLQuery_node1737775161344")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1737775161344, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737775099470", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1737775271526 = glueContext.getSink(path="s3://stedi-lake-house-project-dg/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1737775271526")
CustomerTrusted_node1737775271526.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="customer_trusted")
CustomerTrusted_node1737775271526.setFormat("json")
CustomerTrusted_node1737775271526.writeFrame(SQLQuery_node1737775161344)
job.commit()