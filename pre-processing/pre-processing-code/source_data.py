import json
import csv
from datetime import date, timedelta, datetime, timezone
import boto3
import os
import time
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
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

        source_dataset_url = 'https://delphi.cmu.edu/epidata/api.php?source=covidcast&data_source=' + data_source + '&signal=' + signal + '&time_type=' + \
            time_type + '&geo_type=' + geo_type + '&time_values=' + \
            start.strftime('%Y%m%d') + '-' + \
            step.strftime('%Y%m%d') + '&geo_value=*'

        # Response to Delphi API

        response = None

        retries = 5
        for attempt in range(retries):

            try:
                response = urlopen(source_dataset_url)
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

        # Convering response to json
        data = json.load(response)

        # In the Delphi API, the value of `1` under the `result` key means a valid set of data was returned
        if data['result'] == 1:
            complete_data = complete_data + data['epidata']

        else:
            print(data['result'], 'Failed to fetch ' + filename +
                  ' from ' + start.strftime('%Y%m%d') + ' to ' + step.strftime('%Y%m%d'))

        # Increments the date range by the `days_pre_step` value
        start = start + timedelta(days=days_pre_step)
        step = step + timedelta(days=days_pre_step)

    jsonl_location = '/tmp/jsonl~' + filename + '.jsonl'
    csv_location = '/tmp/csv~' + filename + '.csv'

    # saves data to jsonl file
    with open(jsonl_location, 'w', encoding='utf-8') as j:
        j.write('\n'.join(json.dumps(datum) for datum in complete_data))

    # saves data to csv file
    with open(csv_location, 'w', encoding='utf-8') as c:
        writer = csv.DictWriter(c, fieldnames=complete_data[0])
        writer.writeheader()
        writer.writerows(complete_data)

    # uploads csv and jsonl to s3 and delete from local device

    complete_jsonl_key = new_s3_key + 'jsonl/' + \
        filename.replace('~', '/') + '.jsonl'

    complete_csv_key = new_s3_key + 'csv/' + \
        filename.replace('~', '/') + '.csv'

    s3 = boto3.client('s3')

    s3.upload_file(jsonl_location, s3_bucket, complete_jsonl_key)
    os.remove(jsonl_location)
    s3.upload_file(csv_location, s3_bucket, complete_csv_key)
    os.remove(csv_location)

    print('Uploaded ' + filename)

    return [
        {'Bucket': s3_bucket, 'Key': complete_jsonl_key},
        {'Bucket': s3_bucket, 'Key': complete_csv_key}
    ]


def source_dataset():

    # Response from covidcast_meta enpoint in Delphi API

    meta_url = 'https://delphi.cmu.edu/epidata/api.php?source=covidcast_meta'
    response = None

    retries = 5
    for attempt in range(retries):

        try:
            response = urlopen(meta_url)
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
    data = json.load(response)

    # In the Delphi API, the value of `1` under the `result` key means a valid set of data was returned
    if data['result'] == 1:

        s3 = boto3.client('s3')

        objects = s3.list_objects_v2(
            Bucket=s3_bucket, Prefix=('{}csv/'.format(new_s3_key)))

        keys = {}

        for obj in objects['Contents']:
            key = obj['Key'].split(
                '{}csv/'.format(new_s3_key), 1)[1].split('.csv', 1)[0]
            keys[key] = obj['LastModified']

        existing_meta = []
        update_meta = []

        for meta in data['epidata']:
            if meta['data_source'] != 'nchs-mortality':
                last_updated = datetime.fromtimestamp(
                    meta['last_update'], timezone.utc)
                meta_key = '{}/{}/{}/{}'.format(meta['data_source'],
                                                meta['signal'], meta['time_type'], meta['geo_type'])

                if meta_key in keys:
                    if last_updated > keys[meta_key]:
                        update_meta.append(meta)
                    else:
                        existing_meta = existing_meta + [{'Bucket': s3_bucket, 'Key': '{}csv/{}.csv'.format(
                            new_s3_key, meta_key)}, {'Bucket': s3_bucket, 'Key': '{}jsonl/{}.jsonl'.format(new_s3_key, meta_key)}]
                else:
                    update_meta.append(meta)

        if len(existing_meta) > 0 and len(update_meta) == 0:
            return []

        # mutlithreading to run multiple requests to the covidcast api enpoint
        # in parallel to each other
        with Pool(16) as p:
            asset_lists = p.map(query_and_save_api, update_meta)

        flat_list = [
            asset for asset_list in asset_lists for asset in asset_list]

        # Saves meta data to csv file
        with open('/tmp/csv~covidcast_meta.csv', 'w', encoding='utf-8') as c:
            writer = csv.DictWriter(c, fieldnames=data['epidata'][0])
            writer.writeheader()
            writer.writerows(data['epidata'])

        # Saves meta data to jsonl file
        with open('/tmp/jsonl~covidcast_meta.jsonl', 'w', encoding='utf-8') as j:
            j.write('\n'.join(json.dumps(datum) for datum in data['epidata']))

        # uploads meta files

        s3 = boto3.client('s3')

        for filename in os.listdir('/tmp'):
            if '~covidcast_meta' in filename:
                complete_key = new_s3_key + filename.replace('~', '/')

                s3.upload_file('/tmp/' + filename, s3_bucket, complete_key)
                os.remove('/tmp/' + filename)

                flat_list.append({'Bucket': s3_bucket, 'Key': complete_key})

        print('Uploaded covidcast-meta')

        return flat_list + existing_meta
