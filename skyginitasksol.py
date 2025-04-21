# Import the MySQL connector library to establish a connection and run SQL commands
import mysql.connector
# Import pandas library to load CSV files into DataFrames and work with tabular data
import pandas as pd

# Load the financial information CSV file into a DataFrame
# index_col=0 drops the unwanted first column that pandas creates
# parse_dates turns the specified columns into datetime objects
financial_info = pd.read_csv(r'C:\Users\sonuk\Downloads\finanical_information.csv',index_col=0,
    parse_dates=['start_date', 'end_date'])

# Load subscription information into a DataFrame and parse dates for proper handling
subscription_info = pd.read_csv(r'C:\Users\sonuk\Downloads\subscription_information.csv',parse_dates=['start_date', 'end_date'])

# Load industry client details from CSV into a DataFrame
industry_client_details = pd.read_csv(r'C:\Users\sonuk\Downloads\industry_client_details.csv')

# Load payment information into a DataFrame and convert payment_date to datetime
payment_info = pd.read_csv(r'C:\Users\sonuk\Downloads\payment_information.csv',parse_dates=['payment_date'])

# Establish a connection to the MySQL server using provided credentials
connection = mysql.connector.connect(host='localhost',         # server address (localhost means your local machine)
    user='root',              # your MySQL username
    password='password',      # your MySQL password (update if different)
    database='data_engineer_db'  # the specific database/schema to use
)
# Create a cursor object for executing SQL statements
cursor = connection.cursor()

# Remove the financial_info table if it already exists to avoid conflicts
cursor.execute("DROP TABLE IF EXISTS financial_info")
# Create a new financial_info table with appropriate columns and data types
cursor.execute(
    "CREATE TABLE financial_info ("
    "id INT AUTO_INCREMENT PRIMARY KEY, "      # unique ID for each row
    "start_date DATE, "                         # when the economic period starts
    "end_date DATE, "                           # when the economic period ends
    "inflation_rate FLOAT, "                    # recorded inflation rate
    "gdp_growth_rate FLOAT)"                    # recorded GDP growth rate
)
# Save the table creation to the database
connection.commit()

# Remove the subscription_info table if it exists
cursor.execute("DROP TABLE IF EXISTS subscription_info")
# Create a fresh subscription_info table for client subscription records
cursor.execute(
    "CREATE TABLE subscription_info ("
    "client_id BIGINT, "          # ID linking to the client in industry_client_details
    "subscription_type VARCHAR(20), " # type of subscription (e.g., Basic, Premium)
    "start_date DATE, "             # subscription start date
    "end_date DATE, "               # subscription end date
    "renewed BOOLEAN)"               # whether the client renewed (True/False)
)
connection.commit()

# Remove the industry_client_details table if it exists
cursor.execute("DROP TABLE IF EXISTS industry_client_details")
# Create a new industry_client_details table for client metadata
cursor.execute(
    "CREATE TABLE industry_client_details ("
    "client_id BIGINT, "          # unique client identifier
    "company_size VARCHAR(20), "  # size of the company (e.g., Small, Medium, Large)
    "industry VARCHAR(50), "      # industry sector (e.g., Finance Lending)
    "location VARCHAR(50))"       # client location (city, region, etc.)
)
connection.commit()

# Remove the payment_info table if it exists
cursor.execute("DROP TABLE IF EXISTS payment_info")
# Create a new payment_info table to track client payments
cursor.execute(
    "CREATE TABLE payment_info ("
    "client_id BIGINT, "          # client identifier matching industry_client_details
    "payment_date DATE, "         # date when payment was made
    "amount_paid FLOAT, "         # amount paid by the client
    "payment_method VARCHAR(30))" # payment method used (e.g., Credit Card)
)
connection.commit()

# Define a function that inserts all rows from a DataFrame into a MySQL table
def insert_dataframe_to_table(df, table_name):
    # Make a copy of the DataFrame to avoid modifying the original
    df_clean = df.copy()
    # Drop any pandas-generated index columns like 'Unnamed: 0'
    df_clean = df_clean.loc[:, ~df_clean.columns.str.contains(r'^Unnamed')]
    # If loading into financial_info, drop any 'id' column so MySQL auto-increments
    if table_name == 'financial_info' and 'id' in df_clean.columns:
        df_clean = df_clean.drop(columns=['id'])
    # Loop through each row and insert into the target table
    for _, row in df_clean.iterrows():
        # Create placeholders for parameterized SQL (one %s per column)
        placeholders = ', '.join(['%s'] * len(row))
        # List of column names separated by commas
        columns = ', '.join(row.index)
        # Build the INSERT SQL statement dynamically
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        # Execute the INSERT with actual row values
        cursor.execute(sql, tuple(row))
    # Commit all inserts to the database so they're saved
    connection.commit()

# Use the helper function to load data into each table
insert_dataframe_to_table(financial_info, 'financial_info')
insert_dataframe_to_table(subscription_info, 'subscription_info')
insert_dataframe_to_table(industry_client_details, 'industry_client_details')
insert_dataframe_to_table(payment_info, 'payment_info')

# Query Q1: count clients in Finance Lending or Blockchain industries
# This query counts how many clients belong to the "Finance Lending" or "Block Chain" industry.
# It looks at the "industry" column in the industry_client_details table and counts the rows that match.

cursor.execute(
    "SELECT COUNT(*) FROM industry_client_details "
    "WHERE industry IN ('Finance Lending', 'Block Chain')"
)
# Fetch the single integer result from the query
q1_result = cursor.fetchone()[0]

# Query Q2: find industry with the highest subscription renewal rate
# This query finds the industry that has the highest renewal rate.
# It joins subscription_info with industry_client_details using the client_id.
# Then, it groups the data by industry and calculates the average value of the 'renewed' column.
# Since 'renewed' is stored as boolean (True = 1, False = 0), the average gives the renewal percentage.
# Finally, it sorts the result by this percentage in descending order and picks the top one.


cursor.execute(
    "SELECT i.industry, AVG(s.renewed) AS renewal_rate "
    "FROM subscription_info s "
    "JOIN industry_client_details i ON s.client_id = i.client_id "
    "GROUP BY i.industry "
    "ORDER BY renewal_rate DESC LIMIT 1"
)
# Store the industry name and renewal fraction
q2_industry, q2_rate = cursor.fetchone()

# Query Q3: calculate average inflation rate during renewed subscriptions
# This query calculates the average inflation rate during renewed subscriptions.
# It joins the subscription_info table with financial_info table.
# The join condition ensures we only consider renewed subscriptions (s.renewed = TRUE)
# and check if the subscription start_date falls within a known financial period.

cursor.execute(
    "SELECT AVG(f.inflation_rate) "
    "FROM subscription_info s "
    "JOIN financial_info f ON s.renewed = TRUE "
    "AND s.start_date BETWEEN f.start_date AND f.end_date"
)
# Retrieve the floating-point result
q3_result = cursor.fetchone()[0]

# Query Q4: compute median payment per year using SQL window functions
# This query finds the median amount paid each year.
# Since MySQL doesn't have a direct MEDIAN function, we simulate it using ROW_NUMBER and COUNT in a subquery:
# 1. First, extract year, amount_paid, row number, and total count for each year.
# 2. Then select the middle row(s) depending on if the count is odd or even:
#    - If count is odd: the middle row is (count+1)/2
#    - If count is even: average of (count/2) and (count/2 + 1)
# 3. Finally, take the average of those middle values to get the median.


cursor.execute(
    "SELECT year, AVG(amount_paid) AS median_amount FROM ("
    "SELECT YEAR(payment_date) AS year, amount_paid, "
    "ROW_NUMBER() OVER (PARTITION BY YEAR(payment_date) ORDER BY amount_paid) AS rn, "
    "COUNT(*) OVER (PARTITION BY YEAR(payment_date)) AS cnt "
    "FROM payment_info) ranked "
    "WHERE rn IN (FLOOR((cnt+1)/2), CEIL((cnt+1)/2)) "
    "GROUP BY year ORDER BY year"
)
# Get all rows of (year, median_amount)
q4_rows = cursor.fetchall()

# Print the results to the console for review
print("Results:")
print("Finance/Blockchain clients:", q1_result)
print(f"Top renewal industry: {q2_industry} ({q2_rate*100:.2f}%)")
print(f"Avg inflation at renewal: {q3_result:.2f}%")
print("Median payments by year:")
# Loop through each year and its median payment
for year, median in q4_rows:
    print(f"{year}: {median:.2f}")

# Close the cursor to free up resources
cursor.close()
# Close the database connection to end the session
connection.close()
