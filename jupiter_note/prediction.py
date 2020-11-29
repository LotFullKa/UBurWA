import pandas as pd

customers = pd.read_csv("md_customers.csv", sep=';', comment='#', encoding = 'cp1251')
sales = pd.read_csv("sales_data.csv", sep=";")

sales['PGI_DATE'] = pd.to_datetime(sales['PGI_DATE'], format = '%Y%m%d')

if (type(sales['VOL'][0]) == str):
    sales['VOL'] = sales['VOL'].str.replace(',', '.').astype(float)

data2 = sales

data2['year'] = pd.DatetimeIndex(data2['PGI_DATE']).year
data2['month'] = pd.DatetimeIndex(data2['PGI_DATE']).month
data2['day'] = pd.DatetimeIndex(data2['PGI_DATE']).day

data3 = data2.groupby(['SHIP_TO', 'SKU', 'year', 'month'], as_index = False).sum()

data4 = data3[['SKU', 'year', 'month', 'VOL']]
data4 = data4.groupby(['SKU', 'year', 'month'], as_index = False).sum()

customers_ecom = customers[customers['PROXI_CATEG3'] == "E-COM"]
e_com_product = data2[['SHIP_TO', 'SKU','year', 'month', 'day', 'VOL']]

e_com_product = e_com_product[e_com_product['SHIP_TO'].isin(customers_ecom['SHIP_TO'])]
e_com_product = e_com_product[['SHIP_TO', 'SKU', 'year', 'month', 'VOL']]
e_com_product = e_com_product.groupby(['SHIP_TO', 'SKU', 'year', 'month'], as_index = False).sum()

data4 = e_com_product[['SKU', 'year', 'month', 'VOL']]
data4 = data4.groupby(['SKU', 'year', 'month'], as_index = False).sum()

year = 2019

vol_4_19  = data4[(data4['month'] == 4) & (data4['year'] == year)]
vol_10_19 = data4[(data4['month'] == 10) & (data4['year'] == year)]

otn_10_4 = pd.merge(vol_10_19, vol_4_19, on = ['SKU'])

otn_10_4 = otn_10_4[['SKU', 'month_x', 'VOL_x', 'month_y', 'VOL_y']]
otn_10_4['otnoshenie'] = otn_10_4['VOL_x'] / otn_10_4['VOL_y']

otn_10_4 = otn_10_4[['SKU', 'otnoshenie']]

year = 2020
vol_4_20 = data4[(data4['month'] == 4) & (data4['year'] == year)]
vol_4_20 = vol_4_20.merge(otn_10_4, how='outer', on=['SKU'])
awesome_number = vol_4_20.otnoshenie.mean()

e_com_product_ext = data2[['SHIP_TO', 'SKU','year', 'month', 'day', 'VOL']]

e_com_product_ext = e_com_product_ext[e_com_product_ext['SHIP_TO'].isin(customers_ecom['SHIP_TO'])]

e_com_product_ext = e_com_product_ext[e_com_product_ext['year'] == 2020]

e_com_product_ext = e_com_product_ext.merge(otn_10_4, how='outer', on=['SKU'])
e_com_product_ext.otnoshenie = e_com_product_ext.otnoshenie.fillna(awesome_number)
e_com_product_ext = e_com_product_ext[e_com_product_ext['year'] == 2020 ]
e_com_product_ext['FORECAST_VOL'] = e_com_product_ext['VOL'] * e_com_product_ext['otnoshenie']

e_com_product_ext.insert(0, 'TEAM', 'team_41')
e_com_product_ext.insert(1, 'FORECAST_DATE', [int(20201000 + i) for i in e_com_product_ext['day']])

e_com_product_ext = e_com_product_ext[['TEAM', 'FORECAST_DATE', 'SKU', 'SHIP_TO', 'FORECAST_VOL']]

current.execute("DROP TABLE IF EXISTS TD_RES_FORECAST")
e_com_product_ext.to_sql('TD_RES_FORECAST', db)
