import pandas as pd
import numpy as np
import math
from math import *
import os
from os import listdir
from os.path import isfile, join
import time
from datetime import datetime
from tqdm import tqdm

def EF_calculation(v, parameter):
    if v<5:
        return float(parameter.EF)
    else:
        return float((parameter.Alpha * v**2 + parameter.Beta * v + parameter.Gamma + parameter.Delta/v) / \
            (parameter.Epsilon * v**2 + parameter.Zita * v + parameter.Eta) * (1-parameter.RF))

readpath_root = ".//Data_Truck"
readfile = [join(readpath_root, f) for f in listdir(readpath_root)]       #每一天的文件夹列表 \Data_Truck\day

writepath_root = './/Truck_Emission_day'
# writefile = [join(writepath_root, f) for f in listdir(readpath_root)]     # \Truck_Emission\day
os.makedirs(writepath_root, exist_ok=True)

TruckID_root = ".//Data//Truck_information_test.xlsx"
TrueruckID = pd.read_excel(TruckID_root)

NOx_parameter_path = ".//EmissionModel//HDT_NOx_parameter.xlsx"
NOx_parameter = pd.read_excel(NOx_parameter_path)


for date in tqdm(range(len(readfile))):
    filelist = [join(readfile[date], f) for f in listdir(readfile[date])]       # \Data_Truck\day\TruckID
    # writelist = [join(writefile[date], f) for f in listdir(readfile[date])]     # \Truck_Emission\day\TruckID
    
    em_day = []
    
    for ID in tqdm(range(len(filelist))):
        
        data = pd.read_csv(filelist[ID])
        # truckname = os.path.basename(filelist[ID]).split('.')[0]
        truckname = os.path.basename(filelist[ID])[:-4]
        truckinf = TrueruckID[(TrueruckID['uid']==truckname)].reset_index(drop=True)
        
        Parameter = NOx_parameter[(NOx_parameter['weightmin'] <= (truckinf.weight[0]/1000)) & (NOx_parameter['weightmax'] > (truckinf.weight[0]/1000))\
                            & (NOx_parameter['ChinaStandard'] == truckinf.emission[0])]
        
        # 单车数据集过小
        if len(data)<3:
            continue
        
        #计算时间差值
        time_info = data['time_info'].apply(lambda x: time.mktime(time.strptime(str(x), '%Y-%m-%d %H:%M:%S')))
        time_diff = np.diff(time_info)
        time_diff = np.insert(time_diff, 0, 0)
        data['time_diff'] = time_diff
        
        #计算经纬度差值
        longitude_1 = data['longitude'][1:].reset_index(drop=True)
        longitude_2 = data['longitude'][:-1].reset_index(drop=True)
        d_lon = abs(longitude_1 - longitude_2)
        lon_0 = pd.Series([0])
        d_lon_1 = pd.concat([pd.Series(lon_0),d_lon]).reset_index(drop=True)   
        
        
        latitude_1 = data['latitude'][1:].reset_index(drop=True)   
        latitude_2 = data['latitude'][:-1].reset_index(drop=True)   
        d_lat = abs(latitude_1 - latitude_2)
        lat_0 = pd.Series([0])
        # d_lat_1 = lat_0.append(d_lat).reset_index(drop=True)
        d_lat_1 = pd.concat([lat_0,d_lat]).reset_index(drop=True)

        #根据经纬度差值计算距离
        R = 6378.137        #单位是km
        d_lat = d_lat*pi/180
        latitude_1 = latitude_1*pi/180
        latitude_2 = latitude_2*pi/180
        d_lon = d_lon*pi/180
        s = [0]
        for i in range(len(data)-1):
            s.append( 2 * R * asin(sqrt((sin(d_lat[i]/2))**2 + cos(latitude_1[i])*cos(latitude_2[i])*(sin(d_lon[i]/2))**2 )) )
        data['distance'] = s 
        
        # 筛选
        time_idenx = data.query('time_diff > 60').index
        
        data.loc[time_idenx, 'time_diff'] = 30
        if len(time_idenx) == 0:
            pass
        else:
            if time_idenx[-1] < (len(data)-1):
                data.loc[time_idenx, 'distance'] = data.loc[time_idenx+1, 'distance']
            else:
                data.loc[time_idenx[:-1], 'distance'] = data.loc[time_idenx+1, 'distance']
                data.loc[time_idenx[-1], 'distance'] = data.loc[time_idenx-1, 'distance']
        
        # ******************************* Emission calculation ********************** #
        data['NOx'] = 0
        dt = 30
        segment = math.ceil(len(data)/dt)
        if segment>1:
            for i in range(segment-1):
                distance_sum = data.loc[dt*i:dt*(i+1),'distance'].sum()       # 单位：km
                time_sum = data.loc[dt*i:dt*(i+1),'time_diff'].sum() / 3600   # 单位：小时
                ave_speed = distance_sum/time_sum
                EF = EF_calculation(ave_speed, Parameter)
                em = EF * distance_sum
                data.loc[dt*i:dt*(i+1),'NOx'] = em/dt

            distance_sum = data.loc[dt*(i+1): ,'distance'].sum()
            time_sum = data.loc[dt*(i+1): ,'time_diff'].sum() / 3600
            ave_speed = distance_sum/time_sum
            EF = EF_calculation(ave_speed, Parameter)
            em = EF * distance_sum
            data.loc[dt*i:dt*(i+1),'NOx'] = em / len(data.loc[dt*(i+1): ,'distance'])
            
        else:
            distance_sum = data['distance'].sum()
            time_sum = data['time_diff'].sum() / 3600
            ave_speed = distance_sum/time_sum
            EF = EF_calculation(ave_speed, Parameter)
            em = EF * distance_sum
            data.loc[:,'NOx'] = em / len(data)
        
        em_day.append(data)
    EM = pd.concat(em_day)
    EM.to_csv(join(writepath_root, str(data.loc[0,'event_day'])+'.csv'), encoding='utf-8-sig', index=False)