import pandas as pd
import csv
from datetime import datetime, timedelta
import random

# Function to generate random purchase months based on constraints
def generate_purchase_months(create_month):
    num_months = random.choices([1, 3, 4, 5, 6], weights=[10, 40, 20, 10, 20])[0]
    months = []
    current_month = create_month + timedelta(days=30)  # Start purchases from the next month
    for _ in range(num_months):
        months.append(current_month)
        # Move to the next month
        next_month = current_month + timedelta(days=30)
        current_month = datetime(next_month.year, next_month.month, 1)
    return months

# Generate data
user_data = []
base_date = datetime(2023, 11, 1)
for user_id in range(1, 10001):
    create_month = base_date + timedelta(days=30 * ((user_id - 1) % 7))
    create_month = datetime(create_month.year, create_month.month, 1)
    purchase_months = generate_purchase_months(create_month)
    for purchase_month in purchase_months:
        user_data.append([f"{user_id}", create_month, purchase_month])

# Write data to CSV
with open('/Users/elenacellitti/Documents/code/my_analysis/datasets/sample_users.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["user_id", "account_create_month", "purchase_month"])
    for row in user_data:
        writer.writerow([row[0], row[1].strftime('%Y-%m-%d'), row[2].strftime('%Y-%m-%d')])

# Read data into a dataframe
df = pd.read_csv('/Users/elenacellitti/Documents/code/my_analysis/datasets/sample_users.csv')

# Group by user_id and purchase_month, then count the occurrences
# This is to clean up the data as a user can make more than one purchase per month
summary_df = df.groupby(['user_id', 'account_create_month', 'purchase_month']).size().reset_index(name='purchase_count')

# Save csv file 
summary_df.to_csv('/Users/elenacellitti/Documents/code/my_analysis/datasets/sample_users_summarized.csv', index=False)