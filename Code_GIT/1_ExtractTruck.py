import pandas as pd
import numpy as np
import os
from os import listdir
from os.path import isfile, join
from datetime import time, datetime
from tqdm import tqdm

readpath = ".//Data//GPS_sample.xlsx"
datepath = ".//Data_Date"
writepath = ".//Data_Truck"

data_all = pd.read_excel(readpath)
data_date = data_all.groupby(['event_day'],as_index=False)
for group_name,group_data in tqdm(data_date):
    group_data.to_excel(join(datepath, str(group_name)+'.xlsx'), index=False)

datefile = [i[:-5] for i in listdir(datepath)]      # date file list
pathfile = [join(datepath, f) for f in listdir(datepath)]
writefile = [join(writepath, f) for f in datefile]
for i in range(len(writefile)):
    os.makedirs(writefile[i], exist_ok=True)

for i in tqdm(range(len(pathfile))):
    data = pd.read_excel(pathfile[i])
    print(data.columns, data.dtypes)
    # data['time'] = data['time_info'].apply(lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S'))

    data['hour'] = data['time_info'].apply(lambda x: x.hour)
    data['longitude'], data['latitude'] = data['location（国测局）'].str.split(' ').str

    data = data.sort_values(by=['uid', 'time_info'], axis=0, ascending=[True,True])
    print(data.head())
    
    sub_data = data.groupby(['uid'],as_index=False)
    for group_name,group_data in tqdm(sub_data):

        group_data.to_csv(join(writefile[i], group_name+'.csv'), encoding='utf-8-sig', index = False)
