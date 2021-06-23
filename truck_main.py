# import json
import pandas as pd
import datetime
# import requests
from truck_consolidate_lib import get_data, merge_truck, merge_2, \
    calc_total_vol_weight, groupby_cluster, find_suitable_truck, \
    concat_categoricaldata

dt = datetime.date(2021,3,19).isocalendar()[1]

# headers = {"Authorization": "Bearer 001cc4d2-dd3a-4237-895e-21701bed315d"}

# response = requests.get("https://office.smarttradzt.com:8001/buy-shopping-service/sales-quotation/202002210000088", headers=headers)

# data_containers = response.json()

#%% IMPORTING DATA
trucktype_df = get_data('trucktype.json')               # 1) TRUCK TYPE.
truckfleet_df = get_data('truckfleet.json')             # 2) TRUCK FLEET
truckcompany_df = get_data('truckcompany.json')         # 3) TRUCK COMPANY
packaging_df = get_data('packaging.json')               # 4) PACKAGING.
products_df = get_data('products.json')                 # 5) PRODUCTS.
destcluster_df = get_data('destcluster.json')           # 6) DESTINATION LOCATION CLUSTER.
supploc_df = get_data('supploc.json')                   # 7) SUPPLIER LOCATION.
orders_df = get_data('orders.json')                     # 8) ORDERS.
quotations_df = get_data('all_orders.json', key = "orders_to_consolidate")
# CONTINUE HERE..
df_quote_clus = quotations_df.merge(
                                    destcluster_df,
                                    on='destination_zip_code',
                                    how='left')


#%% MERGE 1 - MERGING TRUCK COMPANY AND TRUCK FLEET

df_truckfleet_comp1 = merge_truck(truckfleet_df, truckcompany_df)

#%% MERGE 2 - MERGING PRODUCTS, DESTINATION, SUPPLIER, PACKAGING

df_merged2 = \
    merge_2(orders_df, products_df, destcluster_df, supploc_df, packaging_df)

#df_merged2
#%% CALCULATING TOTAL VOLUME (PER ORDER)

calc_total_vol_weight(df_merged2)

#%% GROUPING BY DELIVERY WINDOW AND DESTINATION CLUSTER (NUMERICAL)

df_groupbycluster = groupby_cluster(df_merged2)

#%% FINDING SUITABLE TRUCKTYPE FOR EVERY CLUSTER

find_suitable_truck(trucktype_df,df_groupbycluster)

#%% CONCATENATE WITH CATEGORICAL DATA

df5_grouped_num_text = concat_categoricaldata(df_merged2, df_groupbycluster)

#%%

for week, trtype in zip(df5_grouped_num_text.index,df5_grouped_num_text['trucktype']):
    # print(week[0])
    # print(trtype)

    for index, row in df_truckfleet_comp1.iterrows():

        if row['week'] == week[0] and row['truck_type']==trtype and row['availability'] == 'Yes':
            df_truckfleet_comp1.loc[index,'availability'] = 'Booked'
            print(f'Truck Booked for {week}')
            df5_grouped_num_text.loc[week,'truck_code'] = row['truck_code']
            df5_grouped_num_text.loc[week,'truck_numplate'] = row['num_plate']
            df5_grouped_num_text.loc[week,'truck_driver'] = row['driver']
            df5_grouped_num_text.loc[week,'truck_comp_zipcode'] = row['logistic_company_zipcode']
            df5_grouped_num_text.loc[week,'delivery_date'] = row['date']
            df5_grouped_num_text.loc[week,'logistic_company'] = row['logistic_company']
            break

        else:
            #print('No Truck found')
            pass
    if pd.isnull(df5_grouped_num_text.loc[week,'truck_code']):
        print(f'No Truck available for {week}')
        df5_grouped_num_text.loc[week,'truck_code'] = 'Not Available'
        df5_grouped_num_text.loc[week,'truck_numplate'] = 'Not Available'
        df5_grouped_num_text.loc[week,'truck_driver'] = 'Not Available'
        df5_grouped_num_text.loc[week,'truck_comp_zipcode'] = 'Not Available'
        df5_grouped_num_text.loc[week,'delivery_date'] = 'Not Available'
        df5_grouped_num_text.loc[week,'logistic_company'] = 'Not Available'


    else:
        pass
#%%
df5_grouped_num_text.drop(['packaging_basearea'], axis= 1, inplace=True)
a=df5_grouped_num_text.columns
df5_grouped_num_text=df5_grouped_num_text[[
'truck_code',
'trucktype',
'delivery_date',
'logistic_company',
'truck_comp_zipcode',
'truck_numplate',
'truck_driver',
'supplier_zipcode',
'destination_code',
'sku_code',
'destination_zip_code',
'order_id',
'totalvolume',
'totalweight']
]
