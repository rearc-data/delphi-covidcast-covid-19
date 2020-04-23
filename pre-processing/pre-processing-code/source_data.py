import json
import csv
from datetime import date, timedelta, datetime
import boto3
import os
from urllib.request import urlopen

def query_and_save_api(meta):

    # Saving key terms from data to variables
    data_source = meta['data_source']
    signal = meta['signal']
    time_type = meta['time_type']
    geo_type = meta['geo_type']
    min_time = str(meta['min_time'])
    max_time = str(meta['max_time'])
    num_locations = meta['num_locations']

    # Constructs `filename` from data params
    filename = data_source + '~' + signal + '~' + time_type + '~' + geo_type

    # Delphi COVIDcast has a max limit of 3650 rows returned per API call
    # `days_pre_step` calculates the max num of days that can be requested per call
    days_pre_step = int(3650 / num_locations)

    # Constructs date variables to be used to keep track of incrementing date windows
    start = datetime.strptime(min_time, '%Y%m%d')
    step = start + timedelta(days=(days_pre_step - 1))

    end = datetime.strptime(max_time, '%Y%m%d')

    # Loop only while the date assigned to `start` is eariler or the same as the date assigned to `end`
    while start <= end:
        
        # Response to Delphi API
        res = urlopen('https://delphi.cmu.edu/epidata/api.php?source=covidcast&data_source=' + data_source + '&signal=' + signal + '&time_type=' +
                      time_type + '&geo_type=' + geo_type + '&time_values=' + start.strftime('%Y%m%d') + '-' + step.strftime('%Y%m%d') + '&geo_value=*')
        
        # Convering response to json
        data = json.load(res)

        # In the Delphi API, the value of `1` under the `result` key means a valid set of data was returned 
        if data['result'] == 1:
                        
            if start.strftime('%Y%m%d') == str(meta['min_time']):    
                with open('/tmp/csv~' + filename + '.csv', 'w', encoding='utf-8') as c:
                    writer = csv.DictWriter(
                        c, fieldnames=data['epidata'][0])
                    writer.writeheader()
                    writer.writerows(data['epidata'])

                with open('/tmp/jsonl~' + filename + '.jsonl', 'w', encoding='utf-8') as j:
                    for datum in data['epidata']:
                        j.write(json.dumps(datum) + '\n') 
            
            else:
                with open('/tmp/csv~' + filename + '.csv', 'a', encoding='utf-8') as c:
                    writer = csv.DictWriter(
                        c, fieldnames=data['epidata'][0])
                    writer.writerows(data['epidata'])

                with open('/tmp/jsonl~' + filename + '.jsonl', 'a', encoding='utf-8') as j:
                    for datum in data['epidata']:
                        j.write(json.dumps(datum) + '\n') 


        else:
            print(data['result'], 'Failed to fetch ' + filename +
                    ' from ' + start.strftime('%Y%m%d') + ' to ' + step.strftime('%Y%m%d'))
        
        # Increments the date range by the `days_pre_step` value
        start = start + timedelta(days=days_pre_step)
        step = step + timedelta(days=days_pre_step)

    print('Finished saving ' + filename + ' data')

def source_dataset(s3_bucket, new_s3_key):

    # Response from covidcast_meta enpoint in Delphi API 
    res = urlopen('https://delphi.cmu.edu/epidata/api.php?source=covidcast_meta')
    
    # Converts response to json
    data = json.load(res)

    # In the Delphi API, the value of `1` under the `result` key means a valid set of data was returned
    if data['result'] == 1:

        # Iterates through the meta data, fetches data and saves a json file based on params in the meta data
        for meta in data['epidata']:
            query_and_save_api(meta)
        
        # Saves meta data to json and csv file
        print('Saving covidcast-meta')

        with open('/tmp/csv~covidcast_meta.csv', 'w', encoding='utf-8') as c:
            writer = csv.DictWriter(c, fieldnames=data['epidata'][0])
            writer.writeheader()
            writer.writerows(data['epidata'])


        with open('/tmp/jsonl~covidcast_meta.jsonl', 'w', encoding='utf-8') as j:
            for datum in data['epidata']:
                j.write(json.dumps(datum) + '\n')

        asset_list = {'csv': [], "jsonl": []}

        # Creates S3 connection
        s3 = boto3.client('s3')

        # Looping through filenames, uploading to S3
        for filename in os.listdir('/tmp'):
            print('Uploading ' + filename)
            
            s3.upload_file('/tmp/' + filename, s3_bucket,
                           new_s3_key + filename.replace('~', '/'))

            if filename.startswith('jsonl'):
                asset_list['jsonl'].append(
                    {'Bucket': s3_bucket, 'Key': new_s3_key + filename.replace('~', '/')})
            else:
                asset_list['csv'].append(
                    {'Bucket': s3_bucket, 'Key': new_s3_key + filename.replace('~', '/')})
                
        return asset_list

    else:
        print('Failed to fetch covidcast_meta data')