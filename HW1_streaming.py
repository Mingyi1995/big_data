

import sys
import csv

def csvRows(filename):
    with open(filename, 'r') as fi:
        reader = csv.DictReader(fi)
        for row in reader:
            yield(row)

product_info = {}
Last_Customer_Id = None

for row in csvRows(sys.argv[1]):
    Product_Id = row['Product ID']
    Item_cost = row['Item Cost']
    Currently_Customer_Id = row['Customer ID']
    if Product_Id not in product_info:
        product_info[Product_Id] = [Product_Id, 1, float(Item_cost),Currently_Customer_Id]
        
    else:
        if product_info[Product_Id][3] != Currently_Customer_Id:
            product_info[Product_Id] = [Product_Id,product_info[Product_Id][1]+1,product_info[Product_Id][2]+float(Item_cost),Currently_Customer_Id]
        else:
            product_info[Product_Id] = [Product_Id,product_info[Product_Id][1],product_info[Product_Id][2]+float(Item_cost), Currently_Customer_Id]

csv_columns = ['Product Id', 'Customer Count', 'Total Revenue']

with open(sys.argv[2], 'w') as outfile:
    writer = csv.writer(outfile, delimiter = ',')
    writer.writerow(csv_columns)
    for key in sorted(product_info):
        writer.writerow(product_info[key][:3])





