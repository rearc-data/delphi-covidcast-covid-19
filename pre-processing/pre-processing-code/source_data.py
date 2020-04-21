import json
import datetime
import time
import boto3
import os
from urllib.request import urlopen

def query_and_save_json(meta):

    # Saving key terms from data to variables
    data_source = meta["data_source"]
    signal = meta["signal"]
    time_type = meta["time_type"]
    geo_type = meta["geo_type"]
    min_time = str(meta["min_time"])
    max_time = str(meta["max_time"])
    num_locations = meta["num_locations"]

    # Constructs `filename` from data params
    filename = data_source + '-' + signal + '-' + time_type + '-' + geo_type

    # Delphi COVIDcast has a max limit of 3650 rows returned per API call
    # `days_pre_step` calculates the max num of days that can be requested per call
    days_pre_step = int(3650 / num_locations)

    # Constructs datetime variables to be used to keep track of incrementing date windows
    start = datetime.datetime(
        int(min_time[0:4]), int(min_time[4:6]), int(min_time[6:8])).date()

    step = start + datetime.timedelta(days=(days_pre_step - 1))

    end = datetime.datetime(
        int(max_time[0:4]), int(max_time[4:6]), int(max_time[6:8])).date()
    
    # List to contain data while incrementing through dates
    data_list = []

    # Loop only while the date assigned to `start` is eariler or the same as the date assigned to `end`
    while start <= end:
        
        # Response to Delphi API
        res = urlopen('https://delphi.cmu.edu/epidata/api.php?source=covidcast&data_source=' + data_source + '&signal=' + signal + '&time_type=' +
                                time_type + '&geo_type=' + geo_type + '&time_values=' + start.strftime('%Y%m%d') + '-' + step.strftime('%Y%m%d') + '&geo_value=*')
        
        # Convering response to json
        data = json.load(res)

        # In the Delphi API, the value of `1` under the `result` key means a valid set of data was returned 
        if data['result'] == 1:
            # Concats the data from this individual API call to the `data_list` list
            data_list = data_list + data['epidata']
        else:
            print(data['result'], 'Failed to fetch ' + filename +
                    ' from ' + start.strftime('%Y%m%d') + ' to ' + step.strftime('%Y%m%d'))
        
        # Increments the date range by the `days_pre_step` value
        start = start + datetime.timedelta(days=days_pre_step)
        step = step + datetime.timedelta(days=days_pre_step)
    
    # If the data exists save the data to a json file
    if len(data_list) > 0:
        print('Saving ' + filename)
        with open('/tmp/' + filename + '.json', 'w', encoding='utf-8') as f:
            json.dump(data_list, f, indent=4)
    else:
        print('No data to save for ' + filename)

def source_dataset(s3_bucket, new_s3_key):

    # Response from covidcast_meta enpoint in Delphi API 
    res = urlopen('https://delphi.cmu.edu/epidata/api.php?source=covidcast_meta')
    
    # Converts response to json
    data = json.load(res)

    # In the Delphi API, the value of `1` under the `result` key means a valid set of data was returned
    if data['result'] == 1:
        # Iterates through the meta data, fetches data and saves a json file based on params in the meta data
        for meta in data['epidata']:
            query_and_save_json(meta)
        
        # Saves meta data to json file
        print('Saving covidcast-meta')
        with open('/tmp/covidcast-meta.json', 'w', encoding='utf-8') as f:
            json.dump(data['epidata'], f, indent=4)
       	
        # Creates S3 connection
        s3 = boto3.client("s3")

        # List to containing new paths to files/objects needed to be included with the ADX revision
        job_list = []

        # Looping through filenames, uploading to S3 and adding entry to `job_list` for file
        for filename in os.listdir('/tmp'):
            print('Uploading ' + filename)
            s3.upload_file('/tmp/' + filename, s3_bucket, new_s3_key + filename)
            job_list.append(
                {
                    'Bucket': s3_bucket,
                    'Key': new_s3_key + filename
                }
            )

        # Returns `job_list` for use in `lambda_handler`
        return job_list