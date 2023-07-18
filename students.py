""" 
Problem Statement:
You are given two datasets containing information about students in a school. The first dataset contains general information about students, including their ID, name, age, and gender. The second dataset contains the students' scores in three subjects: Math, Science, and English. Your task is to perform some analysis on these datasets using PySpark.

Dataset 1: Students Information

student_id: The unique identifier of the student.
name: The name of the student.
age: The age of the student.
gender: The gender of the student.
Dataset 2: Students Scores

student_id: The unique identifier of the student.
math_score: The score of the student in Math.
science_score: The score of the student in Science.
english_score: The score of the student in English.
Your Tasks:

Load both datasets into separate PySpark DataFrames.
Perform some basic exploratory analysis on each dataset, such as:
Count the total number of students in each dataset.
Calculate the average age of students in the first dataset.
Find the minimum and maximum scores in each subject for the second dataset.
Determine the number of male and female students in the first dataset.
Merge the two datasets based on the "student_id" column to create a new DataFrame with the following columns:
student_id: The unique identifier of the student.
name: The name of the student.
age: The age of the student.
gender: The gender of the student.
math_score: The score of the student in Math.
science_score: The score of the student in Science.
english_score: The score of the student in English.
Calculate the total score for each student and add a new column "total_score" to the merged DataFrame.
Calculate the average score for each subject and add a new row at the end of the merged DataFrame to display these average scores.
Save the merged DataFrame as a new CSV file.
You can implement the above tasks using PySpark and its DataFrame API. Please provide the code solution for each task.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, count, when

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Load both datasets into separate PySpark DataFrames
students_info_df = spark.read.csv("path/to/students_info.csv", header=True)
students_scores_df = spark.read.csv("path/to/students_scores.csv", header=True)

# Task 2: Perform basic exploratory analysis on each dataset
# Count the total number of students in each dataset
total_students_info = students_info_df.count()
total_students_scores = students_scores_df.count()
print("Total number of students in Students Information dataset:", total_students_info)
print("Total number of students in Students Scores dataset:", total_students_scores)

# Calculate the average age of students in the first dataset
average_age = students_info_df.select(avg(col("age"))).first()[0]
print("Average age of students:", average_age)

# Find the minimum and maximum scores in each subject for the second dataset
min_math_score = students_scores_df.select(min(col("math_score"))).first()[0]
max_math_score = students_scores_df.select(max(col("math_score"))).first()[0]
min_science_score = students_scores_df.select(min(col("science_score"))).first()[0]
max_science_score = students_scores_df.select(max(col("science_score"))).first()[0]
min_english_score = students_scores_df.select(min(col("english_score"))).first()[0]
max_english_score = students_scores_df.select(max(col("english_score"))).first()[0]

print("Minimum Math score:", min_math_score)
print("Maximum Math score:", max_math_score)
print("Minimum Science score:", min_science_score)
print("Maximum Science score:", max_science_score)
print("Minimum English score:", min_english_score)
print("Maximum English score:", max_english_score)

# Determine the number of male and female students in the first dataset
gender_counts = students_info_df.groupBy("gender").agg(count("gender").alias("count"))
gender_counts.show()

# Task 3: Merge the two datasets based on "student_id" column
merged_df = students_info_df.join(students_scores_df, "student_id")

# Task 4: Calculate the total score for each student and add a new column "total_score"
merged_df = merged_df.withColumn("total_score", col("math_score") + col("science_score") + col("english_score"))

# Task 5: Calculate the average score for each subject and add a new row at the end of the merged DataFrame
average_scores = students_scores_df.agg(
    avg(col("math_score")).alias("avg_math_score"),
    avg(col("science_score")).alias("avg_science_score"),
    avg(col("english_score")).alias("avg_english_score")
)

merged_df = merged_df.union(average_scores)

# Task 6: Save the merged DataFrame as a new CSV file
merged_df.write.csv("path/to/merged_dataset.csv", header=True)
