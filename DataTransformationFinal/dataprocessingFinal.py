from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql.functions import col, year, month, count, avg, max, min, split, lower, regexp_replace, regexp_extract, broadcast, collect_list, trim, initcap
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

# INITIALIZE SPARK SESSION
spark = SparkSession.builder \
    .appName("DataTransformation") \
    .master("local[*]") \
    .getOrCreate()
print("\n SPARK SESSION IS CREATED.\n")

# SET LOG LEVEL TO ERROR TO REDUCE LOG VERBOSITY
spark.catalog.clearCache()  # CLEAR ALL CACHED DATA
spark.sparkContext.setLogLevel("ERROR")

# DEFINE SCHEMAS
ratings_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("ratings_timestamp", TimestampType(), True)  # ASSUMING THE TIMESTAMP IS IN A VALID FORMAT
])

tags_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("tag", StringType(), True),
    StructField("tags_timestamp", TimestampType(), True)  # ASSUMING THE TIMESTAMP IS IN A VALID FORMAT
])

movies_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True)
])

links_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("imdbId", StringType(), True),
    StructField("tmdbId", StringType(), True)
])

# LOAD RATINGS AND TAGS AS TEXT FILES WITH NO HEADERS
ratings_df = spark.read.schema(ratings_schema).option("header", "false").parquet("ukussept2024/Sid/Project/NewApproach1710/DataIngestion/sqoop/fullLoad/ratings/ratings.parquet")
tags_df = spark.read.schema(tags_schema).option("header", "false").parquet("ukussept2024/Sid/Project/NewApproach1710/DataIngestion/sqoop/fullLoad/tags/tags.parquet")

# LOAD MOVIES AND LINKS AS PROPER CSVs WITH HEADERS
movies_df = spark.read.schema(movies_schema).csv("ukussept2024/Sid/Project/NewApproach1710/DataIngestion/csvFiles/movies.csv", header=True)
links_df = spark.read.schema(links_schema).csv("ukussept2024/Sid/Project/NewApproach1710/DataIngestion/csvFiles/links.csv", header=True)

print("\n SUCCESSFULLY LOADED ALL FILES / CSV TO SPARK DATAFRAME.")

# PRINT COLUMN NAMES
#print("\n COLUMNS IN RATINGS DATAFRAME BEFORE CLEANING: ", ratings_df.columns)
#print("\n COLUMNS IN TAGS DATAFRAME BEFORE CLEANING: ", tags_df.columns)
#print("\n COLUMNS IN MOVIES DATAFRAME BEFORE CLEANING: ", movies_df.columns)
#print("\n COLUMNS IN LINKS DATAFRAME BEFORE CLEANING: ", links_df.columns)

# STEP 1: DATA CLEANING
# REMOVE DUPLICATES AND HANDLE MISSING VALUES
print("\n -------- CLEANING ALL DATAFRAMES (REMOVING DUPLICATES AND NULL VALUES) --------")
ratings_cleaned_df = ratings_df.dropDuplicates().dropna(subset=["userId", "movieId", "rating"])
tags_cleaned_df = tags_df.dropDuplicates().dropna(subset=["userId", "movieId", "tag"]).withColumn("tag", lower(F.col("tag")))
movies_cleaned_df = movies_df.dropDuplicates().dropna(subset=["movieId", "title", "genres"]).withColumn("title", lower(F.col("title")))
links_cleaned_df = links_df.dropDuplicates().dropna(subset=["movieId", "imdbId", "tmdbId"])
print("\n -------- FINISHED DATA CLEANING --------")

print("\n -------- STARTING DATA TRANSFORMATION --------")
# STEP 2: NORMALIZING THE MOVIES DATAFRAME
# EXTRACT YEAR FROM TITLE AND NORMALIZE BY REMOVING YEAR FROM TITLE
movies_cleaned_df = movies_cleaned_df.withColumn("year_of_release", regexp_extract(col("title"), r'\((\d{4})\)', 1)) \
    .withColumn("title", regexp_replace(col("title"), r'\s*\(.*?\)', ''))  # REMOVE YEAR FROM THE TITLE

# STEP 1: STANDARDIZE PUNCTUATION - REPLACE MULTIPLE HYPHENS, SPACES WITH A SINGLE SPACE
movies_cleaned_df = movies_cleaned_df.withColumn("title", regexp_replace(col("title"), r'[\s-]+', ' '))

# STEP 2: FIX REVERSED TITLES - MOVE ARTICLES (LIKE "THE", "A", "AN") TO THE BEGINNING
movies_cleaned_df = movies_cleaned_df.withColumn("title", 
    regexp_replace(col("title"), r'^(.*),\s(The|A|An)\s*$', r'\2 \1'))

# STEP 3: TRIM EXTRA WHITESPACE - REMOVE LEADING AND TRAILING SPACES
movies_cleaned_df = movies_cleaned_df.withColumn("title", trim(col("title")))

# STEP 4: CAPITALIZE EACH WORD'S FIRST LETTER IN THE TITLE
movies_cleaned_df = movies_cleaned_df.withColumn("title", initcap(col("title")))

# SHOW THE RESULT AFTER ALL TRANSFORMATIONS
#movies_cleaned_df.show(5)
print("\n -------- MOVIES DATAFRAME FULLY NORMALIZED WITH CAPITALIZED TITLES --------")

# PRINT COLUMN NAMES
#print("\n COLUMNS IN RATINGS DATAFRAME AFTER CLEANING: ", ratings_cleaned_df.columns)
#print("\n COLUMNS IN TAGS DATAFRAME AFTER CLEANING: ", tags_cleaned_df.columns)
#print("\n COLUMNS IN MOVIES DATAFRAME AFTER CLEANING: ", movies_cleaned_df.columns)
#print("\n COLUMNS IN LINKS DATAFRAME AFTER CLEANING: ", links_cleaned_df.columns)

# PRINT SCHEMA OF EACH DATAFRAME AFTER CLEANING AND TRANSFORMATION#
#print("\n SCHEMA OF RATINGS DATAFRAME AFTER TRANSFORMATION:")
#ratings_cleaned_df.printSchema()
#print("\n SCHEMA OF TAGS DATAFRAME AFTER TRANSFORMATION:")
#tags_cleaned_df.printSchema()
#print("\n SCHEMA OF MOVIES DATAFRAME AFTER TRANSFORMATION:")
#movies_cleaned_df.printSchema()
#print("\n SCHEMA OF LINKS DATAFRAME AFTER TRANSFORMATION:")
#links_cleaned_df.printSchema()

# WRITE TO HDFS
print("\n -------- WRITE BACK TO HDFS --------")
# DEFINE OUTPUT PATHS FOR EACH DATAFRAME
ratings_output_path = "ukussept2024/Sid/Project/NewApproach1710/DataTransformation/ratings_cleaned.parquet"
tags_output_path = "ukussept2024/Sid/Project/NewApproach1710/DataTransformation/tags_cleaned.parquet"
movies_output_path = "ukussept2024/Sid/Project/NewApproach1710/DataTransformation/movies_cleaned.parquet"
links_output_path = "ukussept2024/Sid/Project/NewApproach1710/DataTransformation/links_cleaned.parquet"

# WRITE EACH DATAFRAME TO HDFS IN PARQUET FORMAT
ratings_cleaned_df.write.mode("overwrite").parquet(ratings_output_path)
print("\n SUCCESSFULLY WRITTEN RATINGS_CLEANED_DF TO HDFS AT: ", ratings_output_path)

tags_cleaned_df.write.mode("overwrite").parquet(tags_output_path)
print("\n SUCCESSFULLY WRITTEN TAGS_CLEANED_DF TO HDFS AT: ", tags_output_path)

movies_cleaned_df.write.mode("overwrite").parquet(movies_output_path)
print("\n SUCCESSFULLY WRITTEN MOVIES_CLEANED_DF TO HDFS AT: ", movies_output_path)

links_cleaned_df.write.mode("overwrite").parquet(links_output_path)
print("\n SUCCESSFULLY WRITTEN LINKS_CLEANED_DF TO HDFS AT: ", links_output_path)

# STOP THE SPARK SESSION
spark.stop()
print("\n SPARK SESSION STOPPED.")
