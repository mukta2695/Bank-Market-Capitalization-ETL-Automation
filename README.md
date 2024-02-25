In this project, I created an automated system to compile a list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Here's a summary of what I did:

*Logging Progress*: I implemented a function log_progress() to log the code's progress at different stages in a file named code_log.txt.

*Data Extraction*: I inspected the webpage at the provided URL and identified the tabular information under the heading 'By market capitalization'. Then, I wrote a function extract() to extract this data from the webpage located at https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks and saved it to a dataframe.

*Data Transformation*: Using the exchange rate information provided in a CSV file located at https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv, I transformed the dataframe by adding columns for Market Capitalization in GBP, EUR, and INR, rounded to 2 decimal places.

*Data Loading to CSV*: The transformed dataframe was loaded into an output CSV file located at ./Largest_banks_data.csv.

*Data Loading to SQL Database*: Additionally, the transformed dataframe was loaded into an SQL database named Banks.db as a table named Largest_banks.

*Running Queries on Database*: I executed queries on the database table to verify the loaded data.

*Verification of Log Entries*: Finally, I verified that the log entries were completed at all stages by checking the contents of the file code_log.txt.
