#job3
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

# Script generated for node customer_trusted
customer_trusted_node1783403478156 = glueContext.create_dynamic_frame.from_catalog(database="handson-project-database", table_name="customer_trusted", transformation_ctx="customer_trusted_node1783403478156")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1783403479039 = glueContext.create_dynamic_frame.from_catalog(database="handson-project-database", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1783403479039")

# Script generated for node SQL Query
SqlQuery0 = '''
select DISTINCT c.*
FROM myDataSource c
INNER JOIN myDataSource1 a
ON c.email = a.user;
'''
SQLQuery_node1783403485144 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource1":accelerometer_trusted_node1783403479039, "myDataSource":customer_trusted_node1783403478156}, transformation_ctx = "SQLQuery_node1783403485144")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783403485144, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783402311217", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1783403488441 = glueContext.getSink(path="s3://handson-project-1/curated/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1783403488441")
customer_curated_node1783403488441.setCatalogInfo(catalogDatabase="handson-project-database",catalogTableName="customer_curated")
customer_curated_node1783403488441.setFormat("json")
customer_curated_node1783403488441.writeFrame(SQLQuery_node1783403485144)
job.commit()