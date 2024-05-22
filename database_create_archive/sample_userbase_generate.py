import pandas as pd
import csv
from datetime import datetime, timedelta
import random

# Helper function to generate random purchase months
def generate_purchase_months(create_month, num_months):
    months = []
    current_month = create_month
    for _ in range(num_months):
        # Randomly decide if the user makes a purchase this month
        if random.random() < 0.8:  # 80% chance of making a purchase
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
    num_months = random.randint(1, 6)  # Each user can have purchases up to 6 months
    purchase_months = generate_purchase_months(create_month, num_months)
    for purchase_month in purchase_months:
        user_data.append([f"{user_id}", create_month, purchase_month])

# Write data to CSV
with open('/Users/elenacellitti/Documents/code/my_analysis/datasets/sample_users.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["user_id", "account_create_month", "purchase_month"])
    for row in user_data:
        writer.writerow([row[0], row[1].strftime('%Y-%m-%d'), row[2].strftime('%Y-%m-%d')])

# Read data into pandas DataFrame
df = pd.read_csv('/Users/elenacellitti/Documents/code/my_analysis/datasets/sample_users.csv')

# Group by user_id and purchase_month, then count the occurrences
summary_df = df.groupby(['user_id', 'account_create_month', 'purchase_month']).size().reset_index(name='purchase_count')

# Write summarized data to a new CSV
summary_df.to_csv('/Users/elenacellitti/Documents/code/my_analysis/datasets/sample_users_summarized.csv', index=False)
