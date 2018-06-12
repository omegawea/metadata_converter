# -*- coding: utf-8 -*-
"""
Created on Mon Jun 11 16:20:21 2018

@author: SF
"""

#%% X -> features, y -> label 
from __future__ import print_function, division
import pandas as pd
import numpy as np
from os.path import join, isdir, isfile, dirname, abspath
from os import getcwd
from sys import getfilesystemencoding
from nilmtk.datastore import Key
from nilmtk.measurement import LEVEL_NAMES
from nilmtk.utils import check_directory_exists, get_datastore
from nilm_metadata import convert_yaml_to_hdf5, save_yaml_to_datastore
from inspect import currentframe, getfile, getsourcefile
from copy import deepcopy

import pandas as pd 
import numpy as np 
import h5py

from nilmtk.utils import get_datastore
from nilm_metadata import convert_yaml_to_hdf5, save_yaml_to_datastore

TIMESTAMP_COLUMN_NAME = "timestamp"
TIMEZONE = "Asia/Shanghai"
#%%
def convert_pq(p_index, q_index):    
    dataset = pd.read_csv('export.csv', sep=',')    
    dataset.drop_duplicates(subset=["timestamp"], inplace=True)
    dataset["timestamp"] = pd.to_datetime(dataset.timestamp.values, unit='s', utc=True).tz_convert(TIMEZONE)
    meter = dataset[['timestamp', p_index, q_index]].copy()
    meter.columns = pd.MultiIndex.from_tuples([
        ('physical_quantity','type'), 
        ('power','active'), 
#        ('power','apparent'), 
        ('power','reactive')
        ])
    meter.set_index(('physical_quantity','type'), inplace=True, drop=True)
    meter.columns.set_names(('physical_quantity','type'), inplace=True)
    
    meter = meter.convert_objects(convert_numeric=True)
    meter = meter.dropna()
    meter = meter.astype(float)
    meter = meter.sort_index()
#    meter = meter.resample("1S")
#    meter = reindex_fill_na(meter, idx)
    assert meter.isnull().sum().sum() == 0
    return meter

pqenergy = get_datastore('pqenergy.h5', 'HDF', mode='w')
pqenergy.put('/building1/elec/meter1',  convert_pq('aggr_p', 'aggr_q'))
pqenergy.put('/building1/elec/meter2',  convert_pq('aircon_p', 'aircon_q'))
pqenergy.put('/building1/elec/meter3',  convert_pq('hdryer_p', 'hdryer_q'))
pqenergy.put('/building1/elec/meter4',  convert_pq('wboiler_p', 'wboiler_q'))
pqenergy.put('/building1/elec/meter5',  convert_pq('ecooker_p', 'ecooker_q'))
pqenergy.put('/building1/elec/meter6',  convert_pq('dehumid_p', 'dehumid_q'))
pqenergy.put('/building1/elec/meter7',  convert_pq('fridge_p', 'fridge_q'))
pqenergy.put('/building1/elec/meter8',  convert_pq('aheater_p', 'aheater_q'))
pqenergy.put('/building1/elec/meter9',  convert_pq('ciron_p', 'ciron_q'))
pqenergy.put('/building1/elec/meter10', convert_pq('rcooker_p', 'rcooker_q'))
pqenergy.put('/building1/elec/meter11', convert_pq('tv_p', 'tv_q'))
pqenergy.put('/building1/elec/meter12', convert_pq('vhood_p', 'vhood_q'))
pqenergy.put('/building1/elec/meter13', convert_pq('washer_p', 'washer_q'))
save_yaml_to_datastore('metadata_pq/', pqenergy)
pqenergy.close()
#%%
def convert_s(s_index):    
    dataset = pd.read_csv('export.csv', sep=',')    
    dataset.drop_duplicates(subset=["timestamp"], inplace=True)
    dataset["timestamp"] = pd.to_datetime(dataset.timestamp.values, unit='s', utc=True).tz_convert(TIMEZONE)
    meter = dataset[['timestamp', s_index]].copy()
    meter.columns = pd.MultiIndex.from_tuples([
        ('physical_quantity','type'), 
        ('power','active'), 
#        ('power','apparent'), 
#        ('power','reactive')
        ])
    meter.set_index(('physical_quantity','type'), inplace=True, drop=True)
    meter.columns.set_names(('physical_quantity','type'), inplace=True)    
#    meter = meter.convert_objects(convert_numeric=True)
#    meter = meter.dropna()
#    meter = meter.astype(float)
#    meter = meter.sort_index()
#    meter = meter.resample("1S")
#    meter = reindex_fill_na(meter, idx)
#    assert meter.isnull().sum().sum() == 0
    return meter

senergy = get_datastore('senergy.h5', 'HDF', mode='w')
senergy.put('/building1/elec/meter1',  convert_s('aggr_s'))
senergy.put('/building1/elec/meter2',  convert_s('aircon_s'))
senergy.put('/building1/elec/meter3',  convert_s('hdryer_s'))
senergy.put('/building1/elec/meter4',  convert_s('wboiler_s'))
senergy.put('/building1/elec/meter5',  convert_s('ecooker_s'))
senergy.put('/building1/elec/meter6',  convert_s('dehumid_s'))
senergy.put('/building1/elec/meter7',  convert_s('fridge_s'))
senergy.put('/building1/elec/meter8',  convert_s('aheater_s'))
senergy.put('/building1/elec/meter9',  convert_s('ciron_s'))
senergy.put('/building1/elec/meter10', convert_s('rcooker_s'))
senergy.put('/building1/elec/meter11', convert_s('tv_s'))
senergy.put('/building1/elec/meter12', convert_s('vhood_s'))
senergy.put('/building1/elec/meter13', convert_s('washer_s'))
save_yaml_to_datastore('metadata_pq/', senergy)
senergy.close()
#%%