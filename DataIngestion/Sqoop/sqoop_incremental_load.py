import os
from datetime import datetime

# Get current timestamp in the desired format
current_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

# Define the Sqoop command for incremental load (append mode)
incremental_load_cmd_ratings = f"""
sqoop import \
--connect jdbc:postgresql://ec2-18-132-73-146.eu-west-2.compute.amazonaws.com:5432/testdb \
--username consultants --password WelcomeItc@2022 \
--table netflix_sid_ratings \
--m 1 \
--target-dir ukussept2024/Sid/Project/NewApproach1710/DataIngestion/sqoop/incrementalLoad/ratings/{current_timestamp} \
--incremental append \
--check-column timestamp \
--last-value '2024-10-16 01:00:00'
"""

incremental_load_cmd_tags = f"""
sqoop import \
--connect jdbc:postgresql://ec2-18-132-73-146.eu-west-2.compute.amazonaws.com:5432/testdb \
--username consultants --password WelcomeItc@2022 \
--table netflix_sid_tags \
--m 1 \
--target-dir ukussept2024/Sid/Project/NewApproach1710/DataIngestion/sqoop/incrementalLoad/tags/{current_timestamp} \
--incremental append \
--check-column timestamp \
--last-value '2024-10-16 01:00:00'
"""

# Execute the incremental load command
os.system(incremental_load_cmd_ratings)
os.system(incremental_load_cmd_tags)
