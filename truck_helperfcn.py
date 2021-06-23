
# This file is used to create json file from Excel File.

import pandas as pd

from truck_lib import create_json

filename = r'Truck Consolidation_v4.xlsx'

# 1) TRUCK TYPE
trucktype_df = pd.read_excel(filename,sheet_name='Truck Type')
trucktype_df.drop(['truck_height_m','truck_width_m','truck_length_m'],axis=1, inplace=True)
trucktype_df.name = f'{trucktype_df=}'.split('=')[0]
create_json(trucktype_df)

# 2) TRUCK FLEET
truckfleet_df = pd.read_excel(filename,sheet_name='Truck Fleet')
truckfleet_df.name = f'{truckfleet_df=}'.split('=')[0]
create_json(truckfleet_df)

# 3) TRUCK COMPANY
truckcompany_df = pd.read_excel(filename,sheet_name='Logistic Company')
truckcompany_df.name = f'{truckcompany_df=}'.split('=')[0]
create_json(truckcompany_df)

# 4) PACKAGING
packaging_df = pd.read_excel(filename, sheet_name='Packaging')
packaging_df.drop(['packaging_height_cm','packaging_width_cm','packaging_length_cm'],axis=1, inplace=True)
packaging_df.name = f'{packaging_df=}'.split('=')[0]
create_json(packaging_df)

# 5) PRODUCTS
products_df = pd.read_excel(filename, sheet_name='Products')
products_df.name = f'{products_df=}'.split('=')[0]
create_json(products_df)

# 6) DESTINATION LOCATION CLUSTER
destcluster_df = pd.read_excel(filename,sheet_name='Dest_Cluster')
destcluster_df.name = f'{destcluster_df=}'.split('=')[0]
create_json(destcluster_df)

# 7) SUPPLIER LOCATION
supploc_df = pd.read_excel(filename,sheet_name='Supplier Location')
supploc_df.name = f'{supploc_df=}'.split('=')[0]
create_json(supploc_df)

# 8) ORDERS
orders_df = pd.read_excel(filename,sheet_name='Orders')
orders_df.name = f'{orders_df=}'.split('=')[0]
create_json(orders_df)
