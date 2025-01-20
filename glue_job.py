import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Snowflake connection options
snowflake_options = {
    "sfURL": "https://<your_account>.snowflakecomputing.com",
    "sfDatabase": "<your_database>",
    "sfSchema": "<your_schema>",
    "sfWarehouse": "<your_warehouse>",
    "sfRole": "<your_role>",
    "user": "<your_username>",
    "password": "<your_password>"
}

# Read the Airport Dimension table from Snowflake into a Spark DataFrame
airport_dim_df = spark.read \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "dev_airlines_airports_dim") \
    .load()

# Read the Daily Flights data from the Glue catalog
daily_flights_df = spark.read \
    .format("parquet") \
    .option("path", "s3://path-to-your-glue-catalog/airlines_dataset_gds") \
    .load()

# Filter out rows where the departure delay is less than 60 minutes
filtered_flights_df = daily_flights_df.filter(col("depdelay") > 60)

# Perform the join operation for the departure data (matching origin airport ID)
joined_departure_df = filtered_flights_df.join(
    airport_dim_df,
    filtered_flights_df["originairportid"] == airport_dim_df["airport_id"],
    "left"
)

# Select the necessary fields after departure join
selected_departure_df = joined_departure_df.select(
    "carrier", "destairportid", "depdelay", "arrdelay",
    "city", "name", "state"
)

# Apply schema transformation for the departure data
transformed_departure_df = selected_departure_df \
    .withColumnRenamed("city", "dep_city") \
    .withColumnRenamed("name", "dep_airport") \
    .withColumnRenamed("state", "dep_state")

# Perform the join operation for the arrival data (matching destination airport ID)
joined_arrival_df = transformed_departure_df.join(
    airport_dim_df,
    transformed_departure_df["destairportid"] == airport_dim_df["airport_id"],
    "left"
)

# Select the necessary fields after arrival join
selected_arrival_df = joined_arrival_df.select(
    "carrier", "depdelay", "arrdelay", "dep_city", "dep_airport", "dep_state",
    "airport_id", "city", "name", "state"
)

# Apply final schema transformations before writing to Snowflake
final_df = selected_arrival_df \
    .withColumnRenamed("city", "arr_city") \
    .withColumnRenamed("name", "arr_airport") \
    .withColumnRenamed("state", "arr_state") \
    .withColumnRenamed("depdelay", "dep_delay") \
    .withColumnRenamed("arrdelay", "arr_delay")

# Write the final transformed data to Snowflake
final_df.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "daily_flights_fact") \
    .mode("overwrite") \
    .save()

# Commit the job
job.commit()

