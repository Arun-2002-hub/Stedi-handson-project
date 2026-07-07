#job5
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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1783405570361 = glueContext.create_dynamic_frame.from_catalog(database="handson-project-database", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1783405570361")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1783405571194 = glueContext.create_dynamic_frame.from_catalog(database="handson-project-database", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1783405571194")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT s.*,a.x,a.y,a.z
FROM myDataSource s
INNER JOIN myDataSource1 a
ON s.sensorreadingTime = a.timestamp;
'''
SQLQuery_node1783405574916 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":step_trainer_trusted_node1783405571194, "myDataSource1":accelerometer_trusted_node1783405570361}, transformation_ctx = "SQLQuery_node1783405574916")

# Script generated for node ML_CURATED
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783405574916, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783402311217", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
ML_CURATED_node1783405577951 = glueContext.getSink(path="s3://handson-project-1/curated/ml_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="ML_CURATED_node1783405577951")
ML_CURATED_node1783405577951.setCatalogInfo(catalogDatabase="handson-project-database",catalogTableName="ml_curated")
ML_CURATED_node1783405577951.setFormat("json")
ML_CURATED_node1783405577951.writeFrame(SQLQuery_node1783405574916)
job.commit()