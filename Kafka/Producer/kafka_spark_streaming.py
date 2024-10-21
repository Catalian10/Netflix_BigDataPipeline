from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, json_tuple

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaRealTimeAnalytics") \
    .getOrCreate()

# Define the Kafka topics to read from
topics = "viewing_behavior,search_activity,user_feedback"

# Read streaming data from Kafka
kafkaStreamDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "ip-172-31-13-101.eu-west-2.compute.internal:9092") \
    .option("subscribe", topics) \
    .option("startingOffsets", "earliest") \
    .load()

# Define the schema for your incoming data
kafkaStreamDF = kafkaStreamDF.selectExpr("CAST(value AS STRING)")

# Extract fields from JSON data for processing
parsedDF = kafkaStreamDF.select(
    json_tuple(col("value"), "userId", "movieId", "title", "action", "search_query")
).toDF("userId", "movieId", "title", "action", "search_query")

# Count User Actions (start/stop playback)
user_actions_count = parsedDF \
    .filter("action IS NOT NULL") \
    .groupBy("userId", "action") \
    .count() \
    .withColumnRenamed("count", "action_count")

# Track Popular Searches
popular_searches = parsedDF \
    .filter("search_query IS NOT NULL") \
    .groupBy("search_query") \
    .count() \
    .withColumnRenamed("count", "search_count")

# Analyze User Feedback (likes/dislikes)
user_feedback = parsedDF \
    .filter("action IN ('like', 'dislike')") \
    .groupBy("action") \
    .count() \
    .withColumnRenamed("count", "feedback_count")

# Output the results to the console
user_actions_query = user_actions_count.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

popular_searches_query = popular_searches.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

user_feedback_query = user_feedback.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

# Await termination
user_actions_query.awaitTermination()
popular_searches_query.awaitTermination()
user_feedback_query.awaitTermination()
