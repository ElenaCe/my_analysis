# Code to generate a randomised list of users to use for cohort analysis
# Table should contain a row per user and month that they purchased something

import csv
import random
import datetime
from dateutil.relativedelta import relativedelta

# Function to generate random date within the specified range
def random_date(start, end):
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + datetime.timedelta(days=random_days)

# Function to generate user data
def generate_user_data(num_users, start_month, end_month):
    user_data = []
    current_id = 1
    for user_id in range(1, num_users + 1):
        # Randomly select the account creation month within the range
        account_create_month = start_month + relativedelta(months=random.randint(0, (end_month - start_month).days // 30))
        purchase_months = set()
        for month in range(7):  # 7 months from November 2023 to May 2024
            if random.random() < 0.5:  # 50% chance to make a purchase each month
                purchase_month = account_create_month + relativedelta(months=month)
                if purchase_month <= end_month:
                    purchase_months.add(purchase_month.strftime('%Y-%m'))
        for purchase_month in purchase_months:
            revenue = round(random.uniform(10, 500), 2)  # Random revenue between $10 and $500
            user_data.append([current_id, account_create_month.strftime('%Y-%m'), purchase_month, revenue])
        current_id += 1
    return user_data

# Main function
def main():
    num_users = 500
    start_month = datetime.datetime(2023, 11, 1)  # Start date: November 2023
    end_month = datetime.datetime(2024, 5, 1)    # End date: May 2024
    
    user_data = generate_user_data(num_users, start_month, end_month)
    
    # Write the generated data to a CSV file
    with open('cohort_data.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['user_id', 'account_create_month', 'purchase_month', 'revenue'])
        csv_writer.writerows(user_data)

if __name__ == "__main__":
    main()
