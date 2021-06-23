from collections import defaultdict

import json
import pandas as pd

df = pd.read_excel('all_orders.xlsx', sheet_name="sheet2")
# df.dropna(how="all", inplace=True)
# df.dtypes

json_doc = defaultdict(list)

_id = 0
for _id in df.T:
    # print(_id)
    data = df.T[_id]
    # print(data)
    key = data.course

    for elt in json_doc[key]:
        print(elt)
        if elt["quotation_number"] == data.quotation_number:
            # elt[data.student] = data.grade
            break
    else:
        values = {
        'quotation_number': data.quotation_number,
        'requested_delivery_week_window': data.requested_delivery_week_window,
        'destination_zip_code': data.destination_zip_code,
        'supplier_name': data.supplier_name,
        'supplier_zipcode': data.supplier_zipcode,
        'sku_volume_perunit': data.sku_volume_perunit,
        'sku_volume_uom': data.sku_volume_uom,
        'sku_weight_perunit': data.sku_weight_perunit,
        'sku_weight_uom': data.sku_weight_uom,
        'sku_order_quantity': data.sku_order_quantity
        }

        json_doc[key].append(values)


json_formatted_str = json.dumps(json_doc, indent=2)
filename = 'all_orders.json'

with open(filename, 'w') as outfile:

    outfile.write(json_formatted_str)

print(json.dumps(json_doc, indent=4))
