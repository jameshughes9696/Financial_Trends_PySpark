"""
Project: S&P 500 Financial Trends Analysis with PySpark
Author: James Hughes
Date: 2025-06-20

Description:
This project loads and analyzes 5 years of historical stock data
from the S&P 500 companies using PySpark. The analysis focuses on
sector-level trends, volatility, and moving averages to gain insights
into financial market behavior.

Setup:
- Requires Python 3.8+
- Requires PySpark, pandas, matplotlib, seaborn, and yfinance (optional)
- Data sourced from Kaggle’s "all_stocks_5yr.csv"

Usage:
Run this script in a virtual environment with dependencies installed.
Visualizations are generated using matplotlib and seaborn.

"""
# PySpark for big data processing
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, min, max

# Pandas for smaller data manipulations if needed
import pandas as pd

# Matplotlib and Seaborn for plotting
import matplotlib.pyplot as plt
import seaborn as sns

# Other handy imports
import sys
import os

# Import SparkSession to create a Spark environment
from pyspark.sql import SparkSession

# Create a Spark session named "S&P 500 Stock Analysis"
spark = SparkSession.builder \
    .appName("S&P 500 Stock Analysis") \
    .getOrCreate()

# Import the SparkSession class from PySpark
from pyspark.sql import SparkSession

# Create a Spark session with a descriptive application name
spark = SparkSession.builder \
    .appName("S&P 500 Stock Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Load the CSV data into a Spark DataFrame
stocks_df = spark.read.csv(
    "data/all_stocks_5yr.csv",  # Path to the dataset
    header=True,                # Use the first row as column headers
    inferSchema=True            # Automatically detect data types for each column
)

# Print the schema to understand the structure and types of the data
stocks_df.printSchema()

# Display the first 5 rows of the DataFrame
stocks_df.show(5)

# Count the total number of records in the DataFrame
total_records = stocks_df.count()
print(f"Total records: {total_records}")

# Get distinct count of tickers (companies)
distinct_tickers = stocks_df.select("Name").distinct().count()
print(f"Number of unique companies (tickers): {distinct_tickers}")

# Show summary statistics (mean, min, max, etc.) for the 'close' price column
stocks_df.describe("close").show()

# Show distinct years in the dataset (extracting year from the date)
from pyspark.sql.functions import year

years = stocks_df.select(year("date").alias("Year")).distinct().orderBy("Year")
years.show()

# STEP 1: Data Cleaning and Feature Extraction

# Import relevant pyspark functions

from pyspark.sql.functions import col, year, month, dayofmonth

# Remove rows containing null values

clean_df = stocks_df.dropna()

clean_df.show(5)
