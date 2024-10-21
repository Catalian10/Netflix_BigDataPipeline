import os

# Define Sqoop command for full load from PostgreSQL to HDFS
full_load_cmd_ratings = """
sqoop import \
--connect jdbc:postgresql://ec2-18-132-73-146.eu-west-2.compute.amazonaws.com:5432/testdb \
--username consultants --password WelcomeItc@2022 \
--table netflix_sid_ratings \
--m 1 \
--target-dir ukussept2024/Sid/Project/NewApproach1710/DataIngestion/sqoop/fullLoad/ratings \

"""

full_load_cmd_tags = """
sqoop import \
--connect jdbc:postgresql://ec2-18-132-73-146.eu-west-2.compute.amazonaws.com:5432/testdb \
--username consultants --password WelcomeItc@2022 \
--table netflix_sid_tags \
--m 1 \
--target-dir ukussept2024/Sid/Project/NewApproach1710/DataIngestion/sqoop/fullLoad/tags \

"""

# Execute the full load command
os.system(full_load_cmd_ratings)
os.system(full_load_cmd_tags)