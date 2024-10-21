from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType, StringType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ConvertingDataframe") \
    .master("local[*]") \
    .getOrCreate()

# Define the ratings schema
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
    StructField("tags_timestamp", TimestampType(), True)  # Assuming the timestamp is in a valid format
])

fl_rating_df = spark.read.csv("ukussept2024/Sid/Project/NewApproach1710/DataIngestion/sqoop/fullLoad/ratings/part-m-00000", schema=ratings_schema, header=False)
il_rating_df = spark.read.csv("ukussept2024/Sid/Project/NewApproach1710/DataIngestion/sqoop/incrementalLoad/ratings/20241017_104136/part-m-00000", schema=ratings_schema, header=False)
fl_rating_df.printSchema()
il_rating_df.printSchema()

if fl_rating_df.dtypes == il_rating_df.dtypes:
    print("Ratings Schemas are the same")
else:
    print("Ratings Schemas are different")

fl_tags_df = spark.read.csv("ukussept2024/Sid/Project/NewApproach1710/DataIngestion/sqoop/fullLoad/tags/part-m-00000", schema=tags_schema, header=False)
il_tags_df = spark.read.csv("ukussept2024/Sid/Project/NewApproach1710/DataIngestion/sqoop/incrementalLoad/tags/20241017_104136/part-m-00000", schema=tags_schema, header=False)

fl_tags_df.printSchema()
il_tags_df.printSchema()

if fl_tags_df.dtypes == il_tags_df.dtypes:
    print("Tags Schemas are the same")
else:
    print("Tags Schemas are different")


fl_rating_df.write.mode('overwrite').parquet("ukussept2024/Sid/Project/NewApproach1710/DataIngestion/sqoop/fullLoad/ratings/ratings.parquet")
print("\n Full Load Ratings data saved successfully.\n")

il_rating_df.write.mode('append').parquet("ukussept2024/Sid/Project/NewApproach1710/DataIngestion/sqoop/fullLoad/ratings/ratings.parquet")
print("\n Incremental Load Ratings data saved successfully.\n")


fl_tags_df.write.mode('overwrite').parquet("ukussept2024/Sid/Project/NewApproach1710/DataIngestion/sqoop/fullLoad/tags/tags.parquet")
print("\n Full Load Tags data saved successfully.\n")

il_tags_df.write.mode('append').parquet("ukussept2024/Sid/Project/NewApproach1710/DataIngestion/sqoop/fullLoad/tags/tags.parquet")
print("\n Incremental Load Tags data saved successfully.\n")
