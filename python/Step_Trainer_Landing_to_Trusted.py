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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1737781388537 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1737781388537")

# Script generated for node Customer Curated
CustomerCurated_node1737781724353 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_curated", transformation_ctx="CustomerCurated_node1737781724353")

# Script generated for node SQL Query
SqlQuery3792 = '''
select st.sensorReadingTime, st.serialNumber, distanceFromObject
from c
join st 
on c.serialnumber = st.serialnumber;
'''
SQLQuery_node1737782172506 = sparkSqlQuery(glueContext, query = SqlQuery3792, mapping = {"st":StepTrainerLanding_node1737781388537, "c":CustomerCurated_node1737781724353}, transformation_ctx = "SQLQuery_node1737782172506")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1737782172506, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737778233551", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1737782248539 = glueContext.getSink(path="s3://stedi-lake-house-project-dg/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1737782248539")
StepTrainerTrusted_node1737782248539.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1737782248539.setFormat("json")
StepTrainerTrusted_node1737782248539.writeFrame(SQLQuery_node1737782172506)
job.commit()