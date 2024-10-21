from kafka import KafkaProducer
import json
import time
import random
import pandas as pd
from datetime import datetime

# Load and clean the MovieLens dataset
file_path = '/home/ec2-user/UKUSSeptBatch/Siddhesh/Project/DataIngestion/Movielens/movies.csv'
df = pd.read_csv(file_path)

# Clean the data
df['year'] = df['title'].str.extract(r'\((\d{4})\)')[0]
df['title'] = df['title'].str.replace(r'\s*\(.*?\)', '', regex=True).str.strip()
df = df.dropna(subset=['movieId', 'title'])
df['movieId'] = df['movieId'].astype(int)

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers='ip-172-31-13-101.eu-west-2.compute.internal:9092',  # Adjust as necessary
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define user IDs for simulation
user_ids = [f"user_{i}" for i in range(1, 11)]  # user_1 to user_10

# Continuous data generation
while True:
    # Randomly select a user
    userId = random.choice(user_ids)
    
    # Simulate Viewing Behavior
    movie_row = df.sample(n=1).iloc[0]  # Randomly select one movie
    action = random.choice(["start_playback", "stop_playback"])
    viewing_data = {
        "userId": userId,
        "movieId": int(movie_row['movieId']),  # Convert to standard int
        "title": movie_row['title'],
        "action": action,
        "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        "device": random.choice(["mobile", "laptop", "tv"])
    }
    producer.send('viewing_behavior', viewing_data)
    print(f"Sent viewing data: {viewing_data}")

    # Simulate Search and Browsing Activity
    search_query = random.choice(["action movies", "comedy series", "romantic films", "science fiction", "documentaries"])
    search_data = {
        "userId": userId,
        "search_query": search_query,
        "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        "results_shown": random.randint(1, 10)
    }
    producer.send('search_activity', search_data)
    print(f"Sent search activity data: {search_data}")

    # Simulate User Feedback
    feedback_action = random.choice(["like", "dislike"])
    feedback_data = {
        "userId": userId,
        "movieId": int(movie_row['movieId']),  # Convert to standard int
        "action": feedback_action,
        "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    }
    producer.send('user_feedback', feedback_data)
    print(f"Sent user feedback: {feedback_data}")
    
    # Sleep for a short time before sending the next batch
    time.sleep(5)

producer.flush()
