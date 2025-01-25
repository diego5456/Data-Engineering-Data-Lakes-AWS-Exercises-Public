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

# Script generated for node Accelerator Trusted
AcceleratorTrusted_node1737784804248 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="accelerator_trusted", transformation_ctx="AcceleratorTrusted_node1737784804248")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1737784856252 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1737784856252")

# Script generated for node SQL Query
SqlQuery4298 = '''
select *
from st
inner join a
on a.timestamp = st.sensorreadingtime
'''
SQLQuery_node1737785392613 = sparkSqlQuery(glueContext, query = SqlQuery4298, mapping = {"a":AcceleratorTrusted_node1737784804248, "st":StepTrainerTrusted_node1737784856252}, transformation_ctx = "SQLQuery_node1737785392613")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1737785392613, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737778233551", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1737784894754 = glueContext.getSink(path="s3://stedi-lake-house-project-dg/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1737784894754")
MachineLearningCurated_node1737784894754.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1737784894754.setFormat("json")
MachineLearningCurated_node1737784894754.writeFrame(SQLQuery_node1737785392613)
job.commit()