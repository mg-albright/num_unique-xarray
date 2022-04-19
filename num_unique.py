#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 20:11:16 2022

@author: Mary Grace Albright
@title: num_unique
Counts number of unique occurrences in an xarray variable while preserving lat/lon
"""

import xarray as xr
import dask.dataframe as dd

# import dataset
ds = xr.open_dataset('path_to_dataset')

# Will need to extract target variable if there are multiple in dataset

# I don't want to count the zeros, so, here, I'm setting 0 to NaN
ds = ds.where(ds > 0)

# get size of lat, lon, and time
lat = len(ds.lat)
lon = len(ds.lon)
time = len(ds.time)

# convert to array and reshape
ds_array = ds.to_array()
ds_array =  ds_array.values.reshape(time,lat*lon) # values denotes target variable

# convert to dask DataFrame and compute
ds_df = dd.from_array(ds_array).compute()

# calculate number of unique instances
num_unique = ds_df.nunique()

# convert to numpy and reshape back to lat/lon grid
np_ds = num_unique.to_numpy()
new_ds = np_ds.reshape(lat,lon)

# recreate xarray, if needed
unique_xr = xr.Dataset(
    data_vars=dict(
        variable=(["lat","lon"], new_ds)
    ),
    coords=dict(
        lat=(["lat"], lat),
        lon=(["lon"], lon)
    ),
)






