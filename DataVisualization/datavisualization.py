import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load CSV data into Pandas DataFrames
popular_movies_df = pd.read_csv("/home/ec2-user/UKUSSeptBatch/Siddhesh/BD_Project_1/Hive/Output/most_popular_movies.csv")
trend_df = pd.read_csv("/home/ec2-user/UKUSSeptBatch/Siddhesh/BD_Project_1/Hive/Output/trend_analysis_over_time.csv")
top_genre_movies_df = pd.read_csv("/home/ec2-user/UKUSSeptBatch/Siddhesh/BD_Project_1/Hive/Output/top_rated_movies_by_genre.csv")

# Visualization 1: Most Popular Movies (Top Rated)
# Keep only the top 10 movies by number of ratings
top_10_popular_movies = popular_movies_df.nlargest(10, 'rating_count')

plt.figure(figsize=(10, 6))
sns.barplot(x='rating_count', y='m.title', data=top_10_popular_movies, palette='Blues_d')
plt.title('Top 10 Most Popular Movies by Number of Ratings')
plt.xlabel('Number of Ratings')
plt.ylabel('Movie Title')
plt.tight_layout()
plt.savefig("/home/ec2-user/UKUSSeptBatch/Siddhesh/BD_Project_1/DataVisualization/most_popular_movies.png")
print("\n 1st Visualization saved successfully..")

# Visualization 2: Trend Analysis Over Time
# Convert the 'month' column to datetime
trend_df['month'] = pd.to_datetime(trend_df['month'], format='%Y-%m')

# Use a rolling average to smooth the trend line (optional)
trend_df['average_rating_smoothed'] = trend_df['average_rating'].rolling(window=3).mean()

plt.figure(figsize=(10, 6))
sns.lineplot(x='month', y='average_rating_smoothed', data=trend_df, marker='o')
plt.title('Trend of Average Ratings Over Time (Smoothed)')
plt.xlabel('Month')
plt.ylabel('Average Rating')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("/home/ec2-user/UKUSSeptBatch/Siddhesh/BD_Project_1/DataVisualization/rating_trend_over_time.png")

print("\n 2nd Visualization saved successfully..")

top_genre_movies_df = top_genre_movies_df[top_genre_movies_df['f.genres'] != '(no genres listed)']

# Step 2: Handle multiple genres (optional)
top_genre_movies_df['primary_genre'] = top_genre_movies_df['f.genres'].apply(lambda x: x.split('|')[0])

# Step 3: Group by genre and calculate the total rating count for each genre
genre_rating_counts = top_genre_movies_df.groupby('primary_genre')['rating_count'].sum().reset_index()

# Rename columns for clarity
genre_rating_counts.columns = ['Genre', 'Total Rating Count']

# Visualization: Bar Plot for Total Rating Count by Genre
plt.figure(figsize=(12, 6))
sns.barplot(x='Genre', y='Total Rating Count', data=genre_rating_counts, palette='Set2')

# Rotate the x-axis labels for better readability
plt.xticks(rotation=45)
plt.title('Total Rating Count by Genre')
plt.xlabel('Genre')
plt.ylabel('Total Rating Count')
plt.tight_layout()


# Save the figure
plt.savefig("/home/ec2-user/UKUSSeptBatch/Siddhesh/BD_Project_1/DataVisualization/genre_rating_counts.png")

print("Visualizations saved successfully.")
