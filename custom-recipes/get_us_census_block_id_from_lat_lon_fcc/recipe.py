# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

import requests 
import urllib
import time
from dataiku.customrecipe import *
import sys

#disable InsecureRequestWarning.
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

print ('## Running Plugin v0.5.0 ##')

input_name = get_input_names_for_role('input')[0]

# Recipe out

output_ = get_output_names_for_role('output')[0]
output_dataset = dataiku.Dataset(output_)

schema = [
            {'name':'block_group','type':'string'}
            ,{'name':'block_id','type':'string'}
            ,{'name':'tract_id','type':'string'}
            ,{'name':'county_id','type':'string'}
            ,{'name':'county_name','type':'string'}
            ,{'name':'lat','type':'string'}
            ,{'name':'lon','type':'string'}
            ,{'name':'state_code','type':'string'}
            ,{'name':'state_id','type':'string'}
            ,{'name':'state_name','type':'string'}
            ,{'name':'state_centlat','type':'string'}    
            ,{'name':'state_centlon','type':'string'}
            ,{'name':'state_division','type':'string'}
            ,{'name':'state_region','type':'string'}
            ,{'name':'state_areawater','type':'string'}
            ,{'name':'state_arealand','type':'string'}   
            ,{'name':'county_basename','type':'string'}
            ,{'name':'county_centlat','type':'string'}    
            ,{'name':'county_centlon','type':'string'}
            ,{'name':'county_areawater','type':'string'}
            ,{'name':'county_arealand','type':'string'}     
        ]


P_PAUSE = int(get_recipe_config()['param_api_throttle'])
P_LAT = get_recipe_config()['p_col_lat']
P_LON = get_recipe_config()['p_col_lon']
P_CALL_COUNT = 0

P_BENCHMARK = get_recipe_config()['p_benchmark']
P_VINTAGE = get_recipe_config()['p_vintage']

if P_BENCHMARK=="9":
    P_VINTAGE ="910"

print ('[+] BENCHMARK = {} ; VINTAGE = {} '.format(P_BENCHMARK,P_VINTAGE))
    
P_BATCH_SIZE_UNIT = int(get_recipe_config()['param_batch_size'])
if P_BATCH_SIZE_UNIT is None:
    P_BATCH_SIZE_UNIT = 50000
    
strategy = get_recipe_config()['param_strategy']

if get_recipe_config().get('p_id_column', None) is not None and get_recipe_config().get('p_id_column', None) <>'':
    use_column_id=True
    id_column = get_recipe_config().get('p_id_column', None)
    id_as_int = get_recipe_config().get('param_id_as_int', None)
    
    if id_as_int:
        schema.append({'name':id_column,'type':'int'})
    else:
        schema.append({'name':id_column,'type':'string'})
else:
    use_column_id=False

output_dataset.write_schema(schema)

b=-1 
with output_dataset.get_writer() as writer:
    for df in dataiku.Dataset(input_name).iter_dataframes(chunksize= P_BATCH_SIZE_UNIT ):

        b = b +1
        n_b = b * P_BATCH_SIZE_UNIT 

        df = df[abs(df[P_LAT]>0) | abs(df[P_LON]>0)]

        if strategy =='make_unique':
            dfu = df.groupby([P_LAT,P_LON]).count().reset_index()
        else:
            dfu = df.copy()

        n__ = -1
        for v in dfu.to_dict('records'):

            n__ = n__ + 1
            n_record = n_b + n__

            lat = v[P_LAT]
            lon = v[P_LON]
            
            if use_column_id:
                id_ = v[id_column]

            print '%s - processing: (%s,%s)' % (n_record,lat, lon)
            
            # p = {'format': 'json',
            #      'y': lat,
            #      'x': lon,
            #      'benchmark': P_BENCHMARK,
            #      'vintage': P_VINTAGE,
            #      'layers': '10'
            #      }
            
            # Encode parameters
            params = urllib.urlencode(
                {'format': 'json',
                 'y': lat,
                 'x': lon,
                 'benchmark': P_BENCHMARK,
                 'vintage': P_VINTAGE,
                 'layers': '1'
                 }
            )
            # Contruct request URL
            url = 'https://geocoding.geo.census.gov/geocoder/geographies/coordinates?' + params
            # print '%s - processing: (%s,%s,%s)' % (n_record,lat, lon, url)
            
            
            for P_CALL_COUNT in range(0, 4):
                call = requests.get(url, verify=False)
                print '%s - processing: (%s,%s,%s)' % (n_record, lat, lon, call.status_code)
                if call.status_code == 200:
                    data = call.json()
                    try:
                        d = {}

                        s_geo = data['result'][u'geographies'][u'2010 Census Blocks'][0]
                        d['block_group'] = s_geo[u'GEOID']
                        d['block_id'] = s_geo[u'BLOCK']
                        d['tract_id'] = None
                        d['lat'] = lat
                        d['lon'] = lon

                        s_geo = data['result'][u'geographies'][u'States'][0]
                        d['state_code'] = s_geo[u'STUSAB']
                        d['state_id'] = s_geo[u'GEOID']
                        d['state_name'] = s_geo[u'NAME']
                        d['state_centlat'] = s_geo[u'CENTLAT']
                        d['state_centlon'] = s_geo[u'CENTLON']
                        d['state_division'] = s_geo[u'DIVISION']
                        d['state_region'] = s_geo[u'REGION']
                        d['state_areawater'] = s_geo[u'AREAWATER']
                        d['state_arealand'] = s_geo[u'AREALAND']

                        s_geo = data['result'][u'geographies'][u'Counties'][0]
                        d['county_id'] = s_geo[u'GEOID']
                        d['county_name'] = s_geo[u'NAME']
                        d['county_basename'] = s_geo[u'BASENAME']
                        d['county_centlat'] = s_geo[u'CENTLAT']
                        d['county_centlon'] = s_geo[u'CENTLON']
                        d['county_areawater'] = s_geo[u'AREAWATER']
                        d['county_arealand'] = s_geo[u'AREALAND']

                        col_list_ = ['block_group',
                                     'block_id',
                                     'tract_id',
                                     'county_id',
                                     'county_name',
                                     'lat',
                                     'lon',
                                     'state_code',
                                     'state_id',
                                     'state_name',
                                     'state_centlat',
                                     'state_centlon',
                                     'state_division',
                                     'state_region',
                                     'state_areawater',
                                     'state_arealand',
                                     'county_basename',
                                     'county_centlat',
                                     'county_centlon',
                                     'county_areawater',
                                     'county_arealand'
                                     ]

                        if use_column_id is True:
                            if id_as_int:
                                d[id_column] = int(id_)
                            else:
                                d[id_column] = id_

                        writer.write_row_dict(d)
                        break
                    except:
                        print 'Unable to find these coordinates in the US Census API: Record #:%s, lat:%s, lon:%s, url:%s' % (
                            n_record, lat, lon, url)
                else:
                    time.sleep(P_CALL_COUNT*0.500)
            # call = requests.get(url, verify=False)
            time.sleep(P_PAUSE)
