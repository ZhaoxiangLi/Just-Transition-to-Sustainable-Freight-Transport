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
readfile = [join(readpath_root, f) for f in listdir(readpath_root)]       

writepath_root = '.\Emission_grid_day'
writefile = [join(writepath_root, 'NOx'+f[:-4]+'.shp') for f in listdir(readpath_root)]    
os.makedirs(writepath_root, exist_ok=True)

grid_path = ".//GISData//Shpfile//Chinagrid_1km.shp"                     # 1km grid file
# grid_path = r"E:\Paper\FreightEmission\GISData\test\grid_1km.shp"
grid= gpd.read_file(grid_path)

# ************************************  Grid aggregation by hour  ******************* #

for i in tqdm(range(len(readfile))):
    data = pd.read_csv(readfile[i])
    print(readfile[i])
    print(writefile[i])
    data = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data['longitude'], data['latitude'], crs="epsg:4326"))
    merged_data = gpd.sjoin(grid, data, how='left', predicate='contains')

    grouped_data = merged_data.groupby(['FID', 'hour'])['NOx'].sum().reset_index()

    pivot_result = grouped_data.pivot_table(values='NOx', index='FID', columns='hour', aggfunc='sum')
    pivot_result.columns = [str(int(col)) for col in pivot_result.columns]
    pivot_result.fillna(0, inplace=True)

    merged_grid = pd.merge(grid, pivot_result, left_on='FID', right_index=True, how='left').reset_index(drop=True)
    merged_grid.fillna(0, inplace=True)
    print(merged_grid.head())
    merged_grid.to_file(writefile[i], encoding='utf-8')
