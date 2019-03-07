

import sys
import csv

csv_columns = ['Product Id', 'Customer Count', 'Total Revenue']

with open(sys.argv[2], 'w') as outfile:
    writer = csv.writer(outfile, delimiter = ',')
    writer.writerow(csv_columns)
    with open(sys.argv[1], 'r') as file:
        product_info = {}
        unique_customer = {}
        Last_Customer_Id = None
        for line in file:
            data_point = line.split(',') 
            Product_Id = data_point[3].rstrip() 
            Item_cost = data_point[4].rstrip()
            Currently_Customer_Id = data_point[0].rstrip()
            if Product_Id == 'Product ID':
                pass
            else:
                if Product_Id not in product_info:
                    product_info[Product_Id] = [Product_Id, 1, float(Item_cost),Currently_Customer_Id]
                else:
                    if product_info[Product_Id][3] != Currently_Customer_Id:
                        product_info[Product_Id] = [Product_Id,product_info[Product_Id][1]+1,product_info[Product_Id][2]+float(Item_cost),Currently_Customer_Id]
                    else:
                        product_info[Product_Id] = [Product_Id,product_info[Product_Id][1],product_info[Product_Id][2]+float(Item_cost), Currently_Customer_Id]
                writer.writerow(int(Product_Id[1:]),product_info[Product_Id][:3])

# csv_columns = ['Product Id', 'Customer Count', 'Total Revenue']

# with open(sys.argv[2], 'w') as outfile:
#     writer = csv.writer(outfile, delimiter = ',')
#     writer.writerow(csv_columns)
#     for key in sorted(product_info):
#         writer.writerow(product_info[key][:3])





