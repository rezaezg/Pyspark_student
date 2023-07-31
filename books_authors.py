"""
Problem Statement:
You are given a dataset containing information about books and their authors. Each book record consists of the book title, genre, publication year, and the author's name. Your task is to perform some analysis on this dataset using PySpark.

Dataset Description:
The dataset is a CSV file with the following columns:

book_title: The title of the book.
genre: The genre of the book.
publication_year: The year the book was published.
author_name: The name of the book's author.
Your Tasks:

Load the dataset into a PySpark DataFrame.
Perform some basic exploratory analysis on the dataset, such as:
Count the total number of books.
Calculate the number of books published in each year.
Find the most popular genre (the genre with the highest count).
Determine the number of books written by each author.
Find the top 5 authors with the highest number of books.
Calculate the average publication year for all books.
Create a new DataFrame containing books published after the year 2000 and written by the top 3 authors from task 3.
Save the new DataFrame as a new CSV file.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max, desc, avg
from pyspark.sql.types import IntegerType

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Load the dataset into a PySpark DataFrame
df = spark.read.csv("path/to/dataset.csv", header=True)

# Task 2: Perform basic exploratory analysis on the dataset
# Count the total number of books
total_books = df.count()
print("Total number of books:", total_books)

# Calculate the number of books published in each year
books_per_year = df.groupBy("publication_year").agg(count("book_title").alias("book_count")).orderBy("publication_year")
books_per_year.show()

# Find the most popular genre (the genre with the highest count)
most_popular_genre = df.groupBy("genre").agg(count("book_title").alias("genre_count")).orderBy(desc("genre_count")).first()
print("Most popular genre:", most_popular_genre['genre'])

# Determine the number of books written by each author
books_per_author = df.groupBy("author_name").agg(count("book_title").alias("book_count")).orderBy(desc("book_count"))
books_per_author.show()

# Task 3: Find the top 5 authors with the highest number of books
top_authors = books_per_author.limit(5)
top_authors.show()

# Task 4: Calculate the average publication year for all books
average_publication_year = df.select(avg(col("publication_year")).cast(IntegerType())).first()[0]
print("Average publication year for all books:", average_publication_year)

# Task 5: Create a new DataFrame containing books published after the year 2000
# and written by the top 3 authors from Task 3
top_3_authors = top_authors.select("author_name").rdd.flatMap(lambda x: x).collect()
filtered_df = df.filter((col("publication_year") > 2000) & (col("author_name").isin(top_3_authors)))

# Task 6: Save the new DataFrame as a new CSV file
filtered_df.write.csv("path/to/new_dataset.csv", header=True)
