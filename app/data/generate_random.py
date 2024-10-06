import csv
import random
from datetime import datetime, timedelta

# Define the number of records
num_records = 100000

# Define file name
file_name = 'records_new.csv'

# Define a function to generate random datetime
def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))
def next_date(start, end):
    return start + timedelta(days=1)

# Create a list of random table names and types
table_names = ['Customer_Orders',
'Product_Inventory',
'Sales_Transactions',
'Employee_Details',
'Order_Items',
'Supplier_Records',
'Transaction_History',
'Product_Categories',
'Payment_Methods',
'Shipping_Details',
'Customer_Feedback',
'Marketing_Campaigns',
'Product_Reviews',
'Order_Status',
'Inventory_Adjustments',
'Supplier_Contacts',
'Customer_Addresses',
'Employee_Salaries',
'Sales_Regions',
'Product_Manufacturers',
'Order_Returns',
'Shipping_Providers',
'Customer_Referrals',
'Employee_Shifts',
'Product_Discounts',
'Sales_Commissions',
'Order_History',
'Inventory_Levels',
'Supplier_Purchases',
'Product_Attributes',
'Customer_Orders_History',
'Marketing_Leads',
'Sales_Invoices',
'Employee_Performance',
'Product_Stock',
'Order_Packaging',
'Shipping_Labels',
'Customer_Claims',
'Product_Recommendations',
'Order_Payments',
'Inventory_Transfers',
'Supplier_Invoices',
'Customer_Interactions',
'Product_Attributes',
'Sales_Reports',
'Marketing_Expenses',
'Employee_Trainings',
'Product_Bundles',
'Order_Processing',
'Customer_Support_Tickets']
types = ['Duplicates', 'Missing_Downstream', 'Corrupt_Hash']
source = ['Snowflake','Cassandra','Postgres']
priority = ['L','M','H']

# Define the start and end time range for the records
start_time_range = datetime(2024, 6, 1)
end_time_range = datetime(2024, 10, 2)

# Open a CSV file to write data
with open(file_name, 'w', newline='') as csvfile:
    fieldnames = ['DATA_SOURCE','PRIORITY','CHECK_NAME','TABLE_NAME','DATE','STATUS']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    # Write the header
    writer.writeheader()
    custom_weight = [0] * 85 + [1] * 5 + [2] * 10

    for i in range(1, num_records + 1):
        start_time = random_date(start_time_range, end_time_range)
        end_time = random_date(start_time, end_time_range)
        row = {
            # 'Id': i+86739,
            'DATA_SOURCE': random.choice(source),
            'PRIORITY': random.choice(priority),
            'TABLE_NAME': random.choice(table_names),
            'CHECK_NAME': random.choice(types),
            'DATE': end_time.strftime('%Y-%m-%d'),
            'STATUS': random.choice(custom_weight)
        }
        writer.writerow(row)

print(f'CSV file "{file_name}" with {num_records} records has been created.')
