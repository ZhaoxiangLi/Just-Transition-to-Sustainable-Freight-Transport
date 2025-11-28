import pandas as pd
import numpy as np
import math
from math import *
from shapely.geometry import Polygon
import geopandas as gpd
import os
from os import listdir
from os.path import isfile, join
import time
from datetime import datetime
from tqdm import tqdm

readpath_root = './/Truck_Emission_day'
readfile = [join(readpath_root, f) for f in listdir(readpath_root)]       #每一天的排放csv列表 

writepath_root = '.\Emission_grid_day'
writefile = [join(writepath_root, 'NOx'+f[:-4]+'.shp') for f in listdir(readpath_root)]     # 每天网格排放shp根目录
os.makedirs(writepath_root, exist_ok=True)

grid_path = ".//GISData//Shpfile//Chinagrid_1km.shp"                     # 1km网格矢量
# grid_path = r"E:\Paper\FreightEmission\GISData\test\grid_1km.shp"
grid= gpd.read_file(grid_path)

# ************************************  网格集计-分小时集计  ******************* #

for i in tqdm(range(len(readfile))):
    data = pd.read_csv(readfile[i])
    print(readfile[i])
    print(writefile[i])
    data = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data['longitude'], data['latitude'], crs="epsg:4326"))
    # 合并点数据到面数据，根据点所在的面的 ID 进行合并
    merged_data = gpd.sjoin(grid, data, how='left', predicate='contains')

    # 将合并后的数据按照 'id' 和 'hour' 列进行分组，并对 'NOx' 列进行求和
    grouped_data = merged_data.groupby(['FID', 'hour'])['NOx'].sum().reset_index()

    # 使用 pivot_table 函数将按小时分组后的数据透视成新的 DataFrame
    pivot_result = grouped_data.pivot_table(values='NOx', index='FID', columns='hour', aggfunc='sum')

    # 重新命名列名为 0-23
    pivot_result.columns = [str(int(col)) for col in pivot_result.columns]

    # 将 NaN 值替换为 0
    pivot_result.fillna(0, inplace=True)

    # 将结果合并到矢量面数据中
    merged_grid = pd.merge(grid, pivot_result, left_on='FID', right_index=True, how='left').reset_index(drop=True)
    merged_grid.fillna(0, inplace=True)
    print(merged_grid.head())
    # 保存结果到新的 shapefile 文件
    merged_grid.to_file(writefile[i], encoding='utf-8')
    # merged_grid.to_file(join(writepath_root, 'NOx'+os.path.basename(readfile[i])[:-4]+'.shp'), encoding='utf-8')