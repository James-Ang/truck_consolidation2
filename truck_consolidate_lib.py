import json
import pandas as pd
import numpy as np
from datetime import datetime

# Saving to JSON file
def create_json(df):
    #df.to_json('test.json', orient='records')
    text = df.to_json(orient='records')
    json_object = json.loads(text)

    json_formatted_str = json.dumps(json_object, indent = 4)

    filename = df.name.split('_')[0]+'.json'

    with open(filename, 'w') as outfile:

        outfile.write(json_formatted_str)

def get_data(str,key='none'):

    with open(str, 'r') as outfile:
        data = json.load(outfile)    

    # f = open(str, 'r')
    # data = json.load(f)
    # f.close()
    
    if type(data) == list:
        return pd.DataFrame(data)
    
    else:
        return pd.DataFrame(data.get(list(data.keys())[0]))


def unique(list1):
    # insert the list to the set
    list_set = set(list1)
    # convert the set to the list
    unique_list = (list(list_set))
    return unique_list
    #for x in unique_list:
    #    print(x)

def merge_truck(truckfleet_df, truckcompany_df):
    #df_fleet_available = truckfleet_df[truckfleet_df['availability'] == 'Yes']

    df_truckfleet_comp1 = truckfleet_df.merge(
                                            truckcompany_df,
                                            on='logistic_company',
                                            how='left')
    df_truckfleet_comp1.columns

    format='%d-%m-%Y'
    df_truckfleet_comp1['week'] = pd.to_datetime(df_truckfleet_comp1['date'], format=format).apply(lambda x :datetime.date(x).isocalendar()[1])


    df_truckfleet_comp1.drop([
                            'logistic_company_address',
                            'logistic_company_city' ,
                            #'Packaging Length (cm)'
                            ],
                        axis='columns', inplace=True
                        )
    df_truckfleet_comp1.groupby(['week', 'truck_type'])['availability'].count()

    # df1_grouped = df_truckfleet_comp1.groupby(['week', 'truck_type'])['truck_code'].apply(unique)

    return df_truckfleet_comp1


def merge_2(orders_df, products_df, destcluster_df, supploc_df, packaging_df):

    format='%d-%m-%Y'
    orders_df['week'] = orders_df['requested_delivery_week_window']\
        .apply(lambda x: x.split()[0]).apply(lambda x : datetime.strptime(x,format))\
        .apply(lambda x :datetime.date(x).isocalendar()[1])

    df_ord_product2 = orders_df.merge(
                                products_df,
                                on='sku_code',
                                how='left')

    df_ord_product_dest3 = df_ord_product2.merge(
                                            destcluster_df,
                                            on='destination_code',
                                            how='left')
    # df_ord_product_dest3['Destination Zip Code'] = df_ord_product_dest3['Destination Zip Code'].astype('str')
    # df_ord_product_dest3['Destination Zip Code'].dtype

    df_ord_product_dest_supp4 = df_ord_product_dest3.merge(
                                                        supploc_df,
                                                        on='supplier',
                                                        how='left')

    df_merged2 = df_ord_product_dest_supp4.merge(
                                                packaging_df,
                                                on='packaging_code',
                                                how='left')

    #df_merged2.dtypes
    df_merged2[['destination_zip_code', 'supplier_zipcode']] = \
        df_merged2[['destination_zip_code', 'supplier_zipcode']].astype('str')

    return df_merged2


def calc_total_vol_weight(df_merged2):
    m_2_cm_cube = 1e6
    df_merged2['totalvolume'] = df_merged2['sku_order_quantity'] * \
        df_merged2['packaging_capacity_by_volume'] / m_2_cm_cube
    df_merged2['totalvolume_uom'] = 'm3'


    # CALCULATING TOTAL WEIGHT (PER ORDER)
    ton_2_kg = 1e3
    df_merged2['totalweight'] = df_merged2['sku_order_quantity'] * \
        df_merged2['sku_weight'] / ton_2_kg
    df_merged2['totalweight_uom'] = 'ton'
    df_merged2.columns

def groupby_cluster(df_merged2):
    # Groupby cluster, delivery window, supplier
    df_groupbycluster = df_merged2.groupby(['week', 'destination_cluster','supplier']).sum()

    df_groupbycluster.drop(
                    [
                    #'requested_delivery_week_window',
                    'packaging_capacity_by_volume',
                    'packaging_weight_limit',
                    'sku_weight',
                    'loading_time',
                    'unloading_time',
                    'sku_order_quantity',
                    'units_per_sku'
                    ],
                axis='columns', inplace=True
                )
    return df_groupbycluster

def find_suitable_truck(trucktype_df,df_groupbycluster):

    # SORT BY VOLUME AND REINDEX
    trucktype_sorted_by_vol = trucktype_df.sort_values(by=['truck_max_volume']).reset_index(drop=True)
    # df_groupbycluster.columns

    truck_vol = trucktype_sorted_by_vol['truck_max_volume']
    truck_weight = trucktype_sorted_by_vol['full_truck_load']
    truck_type = trucktype_sorted_by_vol['truck_type']

    # Initialise
    vol_cond = []

    # Create Conditions
    for i in range(len(truck_vol)):
        # print(i)
        # print(truck_vol[i])
        vol_cond.append(
                        (df_groupbycluster['totalweight'] <= truck_weight[i]) \
                        & (df_groupbycluster['totalvolume'] <= truck_vol[i])
                        )

    df_groupbycluster['trucktype'] = np.select(vol_cond, truck_type)

def concat_categoricaldata(df_merged2, df_groupbycluster):

    s_order = df_merged2.groupby(['week', 'destination_cluster','supplier'])['order_id'].apply(unique)
    s_dest = df_merged2.groupby(['week', 'destination_cluster','supplier'])['destination_code'].apply(unique)
    s_sku = df_merged2.groupby(['week', 'destination_cluster','supplier'])['sku_code'].apply(unique)
    s_supp_zip_code = df_merged2.groupby(['week', 'destination_cluster','supplier'])['supplier_zipcode'].apply(unique)
    s_dest_zip_code = df_merged2.groupby(['week', 'destination_cluster','supplier'])['destination_zip_code'].apply(unique)

    df5_grouped_num_text = pd.concat([
                            df_groupbycluster,
                            s_order,
                            s_dest,
                            s_sku,
                            s_supp_zip_code,
                            s_dest_zip_code
                            ], axis=1)
    return df5_grouped_num_text
