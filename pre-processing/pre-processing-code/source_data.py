import json
import csv
from datetime import timedelta, datetime
import boto3
import os
import time
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from multiprocessing.dummy import Pool
from io import StringIO
import math

region = os.environ['REGION']
s3_bucket = os.environ['S3_BUCKET']
data_set_arn = os.environ['DATA_SET_ARN']
data_set_id = data_set_arn.split('/', 1)[1]
data_set_name = os.environ['DATA_SET_NAME']
new_s3_key = data_set_name + '/dataset/'

if not s3_bucket:
    raise Exception("'S3_BUCKET' environment variable must be defined!")
if not new_s3_key:
    raise Exception("'DATA_SET_NAME' environment variable must be defined!")

bucket = boto3.resource('s3').Bucket(s3_bucket)
api_base_url = 'https://api.covidcast.cmu.edu/epidata/api.php?endpoint=covidcast'


def download_dataset(source_dataset_url):
    data = None

    retries = 5
    for attempt in range(retries):
        try:
            data = urlopen(source_dataset_url)
        except HTTPError as e:
            if attempt == retries - 1:
                raise Exception('HTTPError: ', e.code, source_dataset_url)
            time.sleep(0.2 * attempt)

        except URLError as e:
            if attempt == retries - 1:
                raise Exception('URLError: ', e.reason, source_dataset_url)
            time.sleep(0.2 * attempt)
        else:
            break

    data = json.load(data)

    if data['result'] == 1:
        return data['epidata']
    else:
        print(data['result'], 'Failed to fetch ' + source_dataset_url)
        return []


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
    filename = '/{}/{}/{}/{}.'.format(data_source, signal, time_type, geo_type)
    print('Starting {}'.format(filename))

    # Delphi COVIDcast has a max limit of 3649 rows returned per API call
    # `time_pre_step` calculates the max num of days that can be requested per call
    time_pre_step = math.floor(3649 / num_locations)

    # Constructs date variables to be used to keep track of incrementing date windows
    if len(min_time) == 8 and time_type == 'day':
        start = datetime.strptime(min_time, '%Y%m%d')
        step = start + timedelta(days=(time_pre_step - 1))
        end = datetime.strptime(max_time, '%Y%m%d')
    elif len(min_time) == 6 and time_type == 'week':
        start = datetime.strptime(min_time + '1', '%G%V%u')
        step = start + timedelta(weeks=(time_pre_step - 1))
        end = datetime.strptime(max_time + '1', '%G%V%u')

    meta_query = '&data_source={}&signal={}&time_type={}&geo_type={}&time_values='.format(
        data_source, signal, time_type, geo_type)
    source_urls = []

    # Loop only while the date assigned to `start` is eariler or the same as the date assigned to `end`
    while start <= end:
        if len(min_time) == 8 and time_type == 'day':
            current_start = start.strftime('%Y%m%d')
            current_end = step.strftime('%Y%m%d')
        elif len(min_time) == 6 and time_type == 'week':
            current_start = start.strftime('%G%V')
            current_end = step.strftime('%G%V')

        current_url = '{}{}{}-{}&geo_value=*'.format(
            api_base_url, meta_query, current_start, current_end)
        source_urls.append(current_url)

        # Increments the date range by the `time_pre_step` value
        if len(min_time) == 8 and time_type == 'day':
            start = start + timedelta(days=time_pre_step)
            step = step + timedelta(days=time_pre_step)
        elif len(min_time) == 6 and time_type == 'week':
            start = start + timedelta(weeks=time_pre_step)
            step = step + timedelta(weeks=time_pre_step)

    with Pool(16) as p:
        complete_data = p.map(download_dataset, source_urls)

    complete_data = [datum for data in complete_data for datum in data]

    jsonl_key = '{}jsonl{}jsonl'.format(new_s3_key, filename)

    jsonl_encode = None
    if len(complete_data) == 0:
        jsonl_encode = ''.encode()
    else:
        jsonl_encode = '\n'.join(json.dumps(datum)
                                 for datum in complete_data).encode()
    bucket.put_object(Body=jsonl_encode, Key=jsonl_key)
    jsonl_encode = None

    csv_key = '{}csv{}csv'.format(new_s3_key, filename)

    csv_encode = None

    if len(complete_data) == 0:
        csv_encode = ''.encode()
    else:
        csv_encode = StringIO()
        writer = csv.DictWriter(csv_encode, fieldnames=complete_data[0])
        writer.writeheader()
        writer.writerows(complete_data)
        complete_data = None
        csv_encode = csv_encode.getvalue().encode()

    bucket.put_object(Body=csv_encode, Key=csv_key)
    csv_encode = None

    print('Uploaded {}'.format(filename))

    return [
        {'Bucket': s3_bucket, 'Key': jsonl_key},
        {'Bucket': s3_bucket, 'Key': csv_key}
    ]


def source_dataset():

    # Response from covidcast_meta enpoint in Delphi API
    meta_url = '{}_meta'.format(api_base_url)
    metadata = None

    retries = 5
    for attempt in range(retries):

        try:
            metadata = urlopen(meta_url)
        except HTTPError as e:
            if attempt == retries - 1:
                raise Exception('HTTPError: ', e.code, meta_url)
            time.sleep(0.2 * attempt)

        except URLError as e:
            if attempt == retries - 1:
                raise Exception('URLError: ', e.reason, meta_url)
            time.sleep(0.2 * attempt)
        else:
            break

    # Converts response to json
    metadata = json.load(metadata)

    # In the Delphi API, the value of `1` under the `result` key means a valid set of data was returned
    if metadata['result'] == 1:
        metadata = metadata['epidata']

        keys = {}
        existing_assets = []
        update_meta = []

        for object in bucket.objects.filter(Prefix=new_s3_key):
            keys[object.key] = object.last_modified.timestamp()

        for meta in metadata:
            # 6/24/21 - nchs-mortality data_source is causing product revision issues,
            # so lets whitelist those datasets for the time being
            if meta['data_source'] != 'nchs-mortality':
                meta_key = '/{}/{}/{}/{}.'.format(
                    meta['data_source'], meta['signal'], meta['time_type'], meta['geo_type'])
                csv_key = '{}csv{}csv'.format(new_s3_key, meta_key)
                jsonl_key = '{}jsonl{}jsonl'.format(new_s3_key, meta_key)
                update = True
                if csv_key in keys and jsonl_key in keys:
                    if keys[csv_key] > meta['last_update'] and keys[jsonl_key] > meta['last_update']:
                        update = False
                if update:
                    update_meta.append(meta)
                else:
                    existing_assets.extend(({'Bucket': s3_bucket, 'Key': csv_key}, {
                                        'Bucket': s3_bucket, 'Key': jsonl_key}))

        if len(existing_assets) > 0 and len(update_meta) == 0:
            dataexchange = boto3.client(
                service_name='dataexchange',
                region_name=region
            )
            last_adx_revision = dataexchange.list_data_set_revisions(
                DataSetId=data_set_id,
                MaxResults=1
            )
            if last_adx_revision['Revisions'][0]['Finalized']:
                return []
            return [existing_assets[i:i + 100] for i in range(0, len(existing_assets), 100)]

        print('Files to be updated', update_meta)

        threads = math.floor(len(update_meta) / 2)
        if threads > 4:
            threads = 4
        if threads == 0:
            threads = 1

        # mutlithreading to run multiple requests to the covidcast api enpoint
        # in parallel to each other
        with Pool(threads) as p:
            asset_lists = p.map(query_and_save_api, update_meta)

        asset_lists = [
            asset for asset_list in asset_lists for asset in asset_list]

        jsonl_key = new_s3_key + 'jsonl/covidcast_meta.jsonl'
        jsonl_encode = '\n'.join(json.dumps(datum)
                                 for datum in metadata).encode()
        bucket.put_object(Body=jsonl_encode, Key=jsonl_key)
        jsonl_encode = None
        asset_lists.append({'Bucket': s3_bucket, 'Key': jsonl_key})

        csv_key = new_s3_key + 'csv/covidcast_meta.csv'
        csv_encode = StringIO()
        writer = csv.DictWriter(csv_encode, fieldnames=metadata[0])
        writer.writeheader()
        writer.writerows(metadata)
        metadata = None
        csv_encode = csv_encode.getvalue().encode()
        bucket.put_object(Body=csv_encode, Key=csv_key)
        csv_encode = None
        asset_lists.append({'Bucket': s3_bucket, 'Key': csv_key})

        print('Uploaded covidcast-meta')
        asset_lists.extend(existing_assets)

        return [asset_lists[i:i + 100] for i in range(0, len(asset_lists), 100)]

    else:
        raise Exception(
            'There was a problem accessing the covidcast_metadata endpoint')
