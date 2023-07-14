"""
Problem Statement:
You are given a dataset containing information about movies. Each movie record consists of the movie title, genre, release year, and average rating. Your task is to perform some analysis on this dataset using PySpark.

Dataset Description:
The dataset is a CSV file with the following columns:

title: The title of the movie.
genre: The genre of the movie.
release_year: The year the movie was released.
average_rating: The average rating given to the movie.
Your Tasks:

Load the dataset into a PySpark DataFrame.
Perform some basic exploratory analysis on the dataset, such as:
Count the total number of movies.
Calculate the average rating for all movies.
Find the minimum and maximum average ratings.
Determine the number of movies in each genre.
Identify the top 5 movies with the highest average ratings.
Identify the top 5 genres with the highest average ratings.
Calculate the average rating for each year and sort the results in descending order.
Save the dataset with an additional column named "rating_category" that categorizes the movies as "High", "Medium", or "Low" based on their average ratings, and save it as a new CSV file.
You can implement the above tasks using PySpark and its DataFrame API. Please provide the code solution for each task.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, count, desc, when
from pyspark.sql.types import StringType

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Load the dataset into a PySpark DataFrame
df = spark.read.csv("path/to/dataset.csv", header=True)

# Count the total number of movies
total_movies = df.count()
print("Total number of movies:", total_movies)

# Calculate the average rating for all movies
average_rating = df.select(avg(col("average_rating"))).first()[0]
print("Average rating for all movies:", average_rating)

# Find the minimum and maximum average ratings
min_rating = df.select(min(col("average_rating"))).first()[0]
max_rating = df.select(max(col("average_rating"))).first()[0]
print("Minimum average rating:", min_rating)
print("Maximum average rating:", max_rating)

# Determine the number of movies in each genre
genre_counts = df.groupBy("genre").agg(count("genre").alias("movie_count"))
genre_counts.show()

# Identify the top 5 movies with the highest average ratings
top_movies = df.orderBy(desc("average_rating")).limit(5)
top_movies.show()

# Identify the top 5 genres with the highest average ratings
top_genres = df.groupBy("genre").agg(avg("average_rating").alias("avg_rating")).orderBy(desc("avg_rating")).limit(5)
top_genres.show()

# Calculate the average rating for each year and sort in descending order
average_rating_per_year = df.groupBy("release_year").agg(avg("average_rating").alias("avg_rating")).orderBy(desc("avg_rating"))
average_rating_per_year.show()

# Categorize movies based on average ratings
df = df.withColumn("rating_category", when(col("average_rating") >= 4.0, "High").when(col("average_rating") >= 3.0, "Medium").otherwise("Low"))
df = df.withColumn("rating_category", col("rating_category").cast(StringType()))

# Save the dataset with the additional "rating_category" column as a new CSV file
df.write.csv("path/to/categorized_dataset.csv", header=True)
