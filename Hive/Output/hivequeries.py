import pandas as pd
from pyhive import hive

# Step 1: Establish a connection to Hive
conn = hive.Connection(host='ip-172-31-1-36.eu-west-2.compute.internal', port=10000, username='your_username', database='default')

# Step 2: Define the queries
queries = {
    "most_popular_movies": """
        SELECT f.movieId, m.title, COUNT(f.userId) AS rating_count, 
               ROUND(AVG(f.rating), 2) AS average_rating
        FROM 
            sept.superset_fact f
        JOIN 
            sept.movies_dimension m ON f.movieId = m.movieId
        GROUP BY 
            f.movieId, m.title
        ORDER BY 
            rating_count DESC
    """,
    
    "top_rated_movies_by_genre": """
        SELECT 
            f.genres,
            f.movieId,
            m.title,
            ROUND(AVG(f.rating), 2) AS average_rating
        FROM 
            sept.superset_fact f
        JOIN 
            sept.movies_dimension m ON f.movieId = m.movieId
        GROUP BY 
            f.genres, f.movieId, m.title 
        ORDER BY 
            f.genres, average_rating DESC
    """,
    
    "trend_analysis_over_time": """
        SELECT
            DATE_FORMAT(ratings_timestamp, 'yyyy-MM') AS month,
            ROUND(AVG(rating), 2) AS average_rating
        FROM
            sept.superset_fact
        GROUP BY
            DATE_FORMAT(ratings_timestamp, 'yyyy-MM')
        ORDER BY
            month
    """
}

# Step 3: Define file paths for CSV output
output_files = {
    "most_popular_movies": 'most_popular_movies.csv',
    "top_rated_movies_by_genre": 'top_rated_movies_by_genre.csv',
    "trend_analysis_over_time": 'trend_analysis_over_time.csv'
}

# Step 4: Execute each query, fetch results, and write to CSV
for query_name, query in queries.items():
    try:
        print(f"Executing query: {query_name}")
        df = pd.read_sql(query, conn)
        output_file = output_files[query_name]
        
        # Write to CSV, overwrite if the file exists
        df.to_csv(output_file, mode='w', index=False)
        print(f"Query result stored in {output_file}")
    
    except Exception as e:
        print(f"Error executing {query_name}: {e}")

# Step 5: Close the connection
conn.close()

print("All queries executed successfully.")
