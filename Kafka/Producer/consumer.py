from kafka import KafkaConsumer, TopicPartition
import json

# Set up Kafka consumer
bootstrap_servers = 'ip-172-31-13-101.eu-west-2.compute.internal:9092'  # Adjust as necessary

# Create the Kafka consumer
consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    enable_auto_commit=True,
    #group_id='netflix_real_time_group',  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # Deserialize JSON messages
    auto_offset_reset='earliest'  # Start reading from the earliest message
)

# Define the topics and the specific partition
topics = ['viewing_behavior', 'search_activity', 'user_feedback']
partition = 0  # Specify the partition number

# Assign the topic partitions to the consumer
partitions = [TopicPartition(topic, partition) for topic in topics]
consumer.assign(partitions)  # Assign the partitions to the consumer

# Print message to indicate listening state
print(f"Listening for messages on {', '.join(topics)} (Partition: {partition})...")

# Consuming messages from Kafka
try:
    for message in consumer:
        topic = message.topic
        data = message.value
        print(f"Received message from topic '{topic}': {data}")
except KeyboardInterrupt:
    print("Consumer stopped manually.")
finally:
    consumer.close()
