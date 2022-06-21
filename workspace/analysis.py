# Databricks notebook source
from pyspark.sql.functions import split, explode, col, udf
from pyspark.sql.types import *
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('temps-demo').getOrCreate()

# Setting storage account connection
container_name = "datasource"
storage_account_name = "<STORAGE-NAME>"
storage_account_access_key = "<STROAGE-KEY>"
spark.conf.set("fs.azure.account.key." + storage_account_name +".blob.core.windows.net",storage_account_access_key)

# COMMAND ----------

# Get stroage data location
ratingsLocation = "wasbs://" + container_name +"@" + storage_account_name + ".blob.core.windows.net/ratings.csv"
moviesLocation = "wasbs://" + container_name +"@" + storage_account_name +".blob.core.windows.net/movies.csv"
# Get ratings and movies data
ratings = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load(ratingsLocation)
movies = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load(moviesLocation)

# COMMAND ----------

# Get stroage data location
ratingsLocation = "wasbs://" + container_name +"@" + storage_account_name + ".blob.core.windows.net/ratings.csv"
moviesLocation = "wasbs://" + container_name +"@" + storage_account_name +".blob.core.windows.net/movies.csv"
# Get ratings and movies data
ratings = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load(ratingsLocation)
movies = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load(moviesLocation)

# COMMAND ----------

movies.show()

# COMMAND ----------

ratings.show()

# COMMAND ----------


