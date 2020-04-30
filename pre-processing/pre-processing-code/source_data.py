import json
import csv
from datetime import date, timedelta, datetime
import boto3
import botocore
import os
from urllib.request import urlopen
from multiprocessing.dummy import Pool

s3_bucket = os.environ['S3_BUCKET']
data_set_name = os.environ['DATA_SET_NAME']
new_s3_key = data_set_name + '/dataset/'

if not s3_bucket:
    raise Exception("'S3_BUCKET' environment variable must be defined!")

if not new_s3_key:
    raise Exception("'DATA_SET_NAME' environment variable must be defined!")

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

    complete_data = []

    while start <= end:
        
        # Response to Delphi API
        res = urlopen('https://delphi.cmu.edu/epidata/api.php?source=covidcast&data_source=' + data_source + '&signal=' + signal + '&time_type=' +
                      time_type + '&geo_type=' + geo_type + '&time_values=' + start.strftime('%Y%m%d') + '-' + step.strftime('%Y%m%d') + '&geo_value=*')
        
        # Convering response to json
        data = json.load(res)

        # In the Delphi API, the value of `1` under the `result` key means a valid set of data was returned 
        if data['result'] == 1:
            complete_data = complete_data + data['epidata']
                        
        else:
            print(data['result'], 'Failed to fetch ' + filename +
                    ' from ' + start.strftime('%Y%m%d') + ' to ' + step.strftime('%Y%m%d'))
        
        # Increments the date range by the `days_pre_step` value
        start = start + timedelta(days=days_pre_step)
        step = step + timedelta(days=days_pre_step)

    # saves data to csv file
    with open('/tmp/csv~' + filename + '.csv', 'w', encoding='utf-8') as c:
        writer = csv.DictWriter(c, fieldnames=complete_data[0])
        writer.writeheader()
        writer.writerows(complete_data)

    # saves data to jsonl file
    with open('/tmp/jsonl~' + filename + '.jsonl', 'w', encoding='utf-8') as j:
        j.write('\n'.join(json.dumps(datum) for datum in complete_data))

    # uploads csv and jsonl to s3 and delete from local device
    s3 = boto3.client('s3')
    s3.upload_file('/tmp/jsonl~' + filename + '.jsonl', s3_bucket, new_s3_key + 'jsonl/' + filename.replace('~', '/') + '.jsonl')
    os.remove('/tmp/jsonl~' + filename + '.jsonl')
    s3.upload_file('/tmp/csv~' + filename + '.csv', s3_bucket, new_s3_key + 'csv/' + filename.replace('~', '/') + '.csv')
    os.remove('/tmp/csv~' + filename + '.csv')

    print('Uploaded ' + filename)

def source_dataset():

    # deletes previous datasets from rearc's internal covidcast bucket 
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    bucket.objects.filter(Prefix=new_s3_key).delete()

    # Response from covidcast_meta enpoint in Delphi API 
    res = urlopen('https://delphi.cmu.edu/epidata/api.php?source=covidcast_meta')
    
    # Converts response to json
    data = json.load(res)

    # asset_lists is used to exec jobs for the adx product revision in lambda_handler
    asset_lists = {}

    # In the Delphi API, the value of `1` under the `result` key means a valid set of data was returned
    if data['result'] == 1:

        # Iterates through the meta data to organize the asset_lists dict to be
        # returned at end of func
        for meta in data['epidata']:
            s3_path = meta['data_source'] + '/' + meta['signal'] + '/' + meta['time_type'] + '/' + meta['geo_type']

            asset_list = [{'Bucket': s3_bucket, 'Key': new_s3_key + 'jsonl/' + s3_path +
                           '.jsonl'}, {'Bucket': s3_bucket, 'Key': new_s3_key + 'csv/' + s3_path + '.csv'}]

            if meta['data_source'] not in asset_lists:
                asset_lists[meta['data_source']] = asset_list

            else:
                asset_lists[meta['data_source']
                            ] = asset_lists[meta['data_source']] + asset_list

        # mutlithreading to run multiple requests to the covidcast api enpoint
        # in parallel to each other
        with Pool(150) as p:
            p.map(query_and_save_api, data['epidata'])

        # Saves meta data to csv file
        with open('/tmp/csv~covidcast_meta.csv', 'w', encoding='utf-8') as c:
            writer = csv.DictWriter(c, fieldnames=data['epidata'][0])
            writer.writeheader()
            writer.writerows(data['epidata'])

        # Saves meta data to jsonl file
        with open('/tmp/jsonl~covidcast_meta.jsonl', 'w', encoding='utf-8') as j:
            j.write('\n'.join(json.dumps(datum) for datum in data['epidata']))
        
        s3 = boto3.client('s3')
        
        # uploads meta files
        for filename in os.listdir('/tmp'):
            if '~covidcast_meta' in filename:
                s3.upload_file('/tmp/' + filename, s3_bucket,
                            new_s3_key + filename.replace('~', '/'))
                os.remove('/tmp/' + filename)
        
        print('Uploaded covidcast-meta')

        # adds meta files to asset_lists dict
        asset_lists['meta'] = [{'Bucket': s3_bucket, 'Key': new_s3_key + 'jsonl/covidcast_meta.jsonl'}, {
            'Bucket': s3_bucket, 'Key': new_s3_key + 'csv/covidcast_meta.csv'}]

        return asset_lists

    else:
        print('Failed to fetch covidcast_meta data')
