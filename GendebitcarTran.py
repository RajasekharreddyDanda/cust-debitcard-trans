import csv
import random
import boto3
from faker import Faker
from datetime import datetime, timedelta

# Function to generate mock data for a single transaction
def generate_transaction():
    fake = Faker()
    customer_id = fake.random_int(min=1000, max=9999)
    name = fake.name()
    debit_card_number = fake.credit_card_number(card_type='visa')
    debit_card_type = 'VISA'  # For simplicity, you can change this to a random card type
    bank_name = fake.random_element(elements=('Bank A', 'Bank B', 'Bank C'))
    transaction_date = fake.date_time_between(start_date="-1y", end_date="now").strftime('%Y-%m-%d %H:%M:%S')
    amount_spend = round(random.uniform(1, 1000), 2)  # Random amount between 1 and 1000
    return [customer_id, name, debit_card_number, debit_card_type, bank_name, transaction_date, amount_spend]

# Function to generate daily transactions and save to CSV
def generate_daily_transactions(date, num_transactions, s3_bucket):
    folder_name = date.strftime('data=%Y-%m-%d')
    s3_key = f'daily_rawdata/{folder_name}/transactions.csv'
    fieldnames = ['customer_id', 'name', 'debit_card_number', 'debit_card_type', 'bank_name', 'transaction_date', 'amount_spend']
    with open('/tmp/transactions.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for _ in range(num_transactions):
            transaction_data = generate_transaction()
            writer.writerow({fieldnames[i]: transaction_data[i] for i in range(len(fieldnames))})
    # Upload file to S3
    s3_client = boto3.client('s3')
    s3_client.upload_file('/tmp/transactions.csv', s3_bucket, s3_key)

def lambda_handler(event, context):
    current_date = datetime.now().date()  # Get the current system date
    num_transactions = 100  # Modify the number of transactions as needed
    s3_bucket = "customer-debitcard-purchases"  # Specify your S3 bucket name
    generate_daily_transactions(current_date, num_transactions, s3_bucket)

