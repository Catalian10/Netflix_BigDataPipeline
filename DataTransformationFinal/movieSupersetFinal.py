from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SupersetDataFrame") \
    .getOrCreate()

# Define schemas based on transformed data (all Parquet files now)
ratings_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("ratings_timestamp", TimestampType(), True)
])

tags_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("tag", StringType(), True),
    StructField("tags_timestamp", TimestampType(), True)
])

movies_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True),  # Load genres as a simple string (already cleaned)
    StructField("year_of_release", StringType(), True)
])

links_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("imdbId", StringType(), True),
    StructField("tmdbId", StringType(), True)
])

# Define paths to the Parquet files in HDFS (as they were stored in the previous step)
ratings_path = 'ukussept2024/Sid/Project/NewApproach1710/DataTransformation/ratings_cleaned.parquet'
tags_path = 'ukussept2024/Sid/Project/NewApproach1710/DataTransformation/tags_cleaned.parquet'
movies_path = 'ukussept2024/Sid/Project/NewApproach1710/DataTransformation/movies_cleaned.parquet'
links_path = 'ukussept2024/Sid/Project/NewApproach1710/DataTransformation/links_cleaned.parquet'

# Load the Parquet files into DataFrames
ratings_df = spark.read.schema(ratings_schema).parquet(ratings_path)
tags_df = spark.read.schema(tags_schema).parquet(tags_path)
movies_df = spark.read.schema(movies_schema).parquet(movies_path)
links_df = spark.read.schema(links_schema).parquet(links_path)

# Join the DataFrames to create the superset DataFrame
superset_df = ratings_df.join(movies_df, on='movieId', how='inner') \
    .join(tags_df, on=['userId', 'movieId'], how='left') \
    .join(links_df, on='movieId', how='left')

superset_df = superset_df.dropna().dropDuplicates()
superset_df.printSchema()

# Show the resulting superset DataFrame
#superset_df.show(5)

# Define the output path
output_path = 'ukussept2024/Sid/Project/NewApproach1710/DataTransformation/movie_superset.parquet'

# Write the superset DataFrame to HDFS in Parquet format
#superset_df.write.mode("overwrite").parquet(output_path)
print(f"\nSuccessfully written the superset DataFrame to {output_path}")

# Stop the Spark session
spark.stop()
