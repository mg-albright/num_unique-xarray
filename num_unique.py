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

# Will need to extract target variable as xarray if there are multiple in dataset

# create function
def num_unique(ds):
    
    # I don't want to count the zeros, so, here, I'm setting 0 to NaN
    ds = ds.where(ds > 0)

    # get size of lat, lon, and time
    n_lat = len(ds.lat)
    n_lon = len(ds.lon)
    n_time = len(ds.time)
    
    # get lat and lon arrays
    lat = ds.lat
    lon = ds.lon

    # convert to array and reshape
    ds_array = ds.to_array()
    ds_array =  ds_array.values.reshape(n_time,n_lat*n_lon) # values denotes target variable

    # convert to dask DataFrame and compute
    ds_df = dd.from_array(ds_array).compute()

    # calculate number of unique instances
    count_num_unique = ds_df.nunique()

    # convert to numpy and reshape back to lat/lon grid
    np_ds = count_num_unique.to_numpy()
    new_ds = np_ds.reshape(n_lat,n_lon)

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
    
    return unique_xr




