# Project Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline using Python to collect, process, and store financial data related to the world’s largest banks by market capitalization.

The pipeline extracts data from a public web source, enriches it using external exchange rate data, and loads the transformed results into both CSV files and a SQLite database, making it suitable for analytics and reporting use cases.


# ETL Architecture

# Extract

Scrapes structured data from a Wikipedia page using:

requests

BeautifulSoup


Extracts the top banks and their market capitalization in USD.

Converts HTML table data into a Pandas DataFrame.

# Transform

Reads external exchange rate data from a CSV file.

Converts market capitalization values from USD into:

GBP

EUR

INR


Applies rounding and numeric transformations.

Produces a clean, enriched dataset ready for analytics.

# Load

Saves transformed data to:

CSV file

SQLite database table


Uses SQLAlchemy for database connectivity.

Executes validation queries to confirm successful loading.


# Logging & Monitoring

Implements a custom logging function to track:

ETL execution stages

Timestamps for each major step


Logs are written to a local log file for traceability and debugging.


# Technologies & Libraries Used

Python

Pandas

NumPy

BeautifulSoup (bs4)

Requests

SQLAlchemy

SQLite

LXML




# How to Run

1. Install required Python libraries.


2. Run the ETL script:

python etl_script.py


3. Output will be:

CSV file with transformed data

SQLite database containing the final table

Console output from validation SQL queries


