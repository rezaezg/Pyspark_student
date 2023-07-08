""" 
You are given a dataset containing information about online customer reviews for a product. Each review consists of the review text and a rating from 1 to 5. Your task is to perform some analysis on this dataset using PySpark.

Dataset Description:
The dataset is a CSV file with the following columns:

review_text: The text content of the customer review.
rating: The rating given by the customer (an integer between 1 and 5).
Your Tasks:

Load the dataset into a PySpark DataFrame.
Perform some basic exploratory analysis on the dataset, such as:
Count the total number of reviews.
Calculate the average rating.
Find the minimum and maximum ratings.
Determine the number of reviews for each rating value.
Preprocess the review text column by performing the following steps:
Remove any leading or trailing spaces.
Convert the text to lowercase.
Remove any punctuation marks or special characters.
Perform sentiment analysis on the preprocessed review text using the "rating" column as the label. Assign a sentiment label of "positive" for ratings greater than or equal to 4, and "negative" for ratings less than 4.
Calculate the distribution of positive and negative reviews.
Save the preprocessed dataset with the sentiment labels as a new CSV file.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, when, count, avg, max, min
from pyspark.sql.types import IntegerType

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Load the dataset into a PySpark DataFrame
df = spark.read.csv("path/to/dataset.csv", header=True)

# Count the total number of reviews
total_reviews = df.count()
print("Total number of reviews:", total_reviews)

# Calculate the average rating
average_rating = df.select(avg(col("rating"))).first()[0]
print("Average rating:", average_rating)

# Find the minimum and maximum ratings
min_rating = df.select(min(col("rating"))).first()[0]
max_rating = df.select(max(col("rating"))).first()[0]
print("Minimum rating:", min_rating)
print("Maximum rating:", max_rating)

# Determine the number of reviews for each rating value
rating_counts = df.groupBy("rating").agg(count("rating").alias("review_count"))
rating_counts.show()

# Preprocess the review text column
df = df.withColumn("review_text", lower(col("review_text")))
df = df.withColumn("review_text", regexp_replace(col("review_text"), "[^a-zA-Z0-9\\s]", ""))

# Perform sentiment analysis and assign sentiment labels
df = df.withColumn("sentiment", when(col("rating") >= 4, "positive").otherwise("negative"))
df = df.withColumn("sentiment", when(col("sentiment") == "positive", 1).otherwise(0))

# Calculate the distribution of positive and negative reviews
sentiment_distribution = df.groupBy("sentiment").agg(count("sentiment").alias("review_count"))
sentiment_distribution.show()

# Save the preprocessed dataset with sentiment labels as a new CSV file
df.write.csv("path/to/preprocessed_dataset.csv", header=True)
