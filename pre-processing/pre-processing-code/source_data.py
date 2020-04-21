import json
import datetime
import time
import requests
import boto3
import os

def query_and_save_json(data_source, signal, time_type, geo_type, min_time, max_time):

        file_name = data_source + '-' + signal + '-' + time_type + '-' + geo_type

        start = datetime.datetime(
            int(min_time[0:4]), int(min_time[4:6]), int(min_time[6:8])).date()

        end = datetime.datetime(
            int(max_time[0:4]), int(max_time[4:6]), int(max_time[6:8])).date()
        
        if time_type == 'day':
            res_list = []

            while start <= end:

                res = requests.get(
                    'https://delphi.cmu.edu/epidata/api.php?source=covidcast&data_source=' + data_source + '&signal=' + signal + '&time_type=' + time_type + '&geo_type=' + geo_type + '&time_values=' + start.strftime('%Y%m%d') + '&geo_value=*').json()
                if res['result'] == 1:
                    res_list = res_list + res['epidata']
                    
                else:
                    print(res['result'], 'Failed to fetch ' +
                          file_name + ' for ' + start.strftime('%Y%m%d'))
                
                start = start + datetime.timedelta(days=1)
            
            if len(res_list) > 0:
                with open('/tmp/' + file_name + '.json', 'w', encoding='utf-8') as f:
                    json.dump(
                        res_list, f, ensure_ascii=False, indent=4)
            else:
                print('End: Failed to fetch ' + file_name)
        else:
            res = requests.get(
                'https://delphi.cmu.edu/epidata/api.php?source=covidcast&data_source=' + data_source + '&signal=' + signal + '&time_type=' + time_type + '&geo_type=' + geo_type + '&time_values=' + start.strftime('%Y%m%d') - end.strftime('%Y%m%d') + '&geo_value=*').json()
            if res['result'] == 1:
                with open('/tmp/' + file_name + '.json', 'w', encoding='utf-8') as f:
                    json.dump(
                        res['epidata'], f, ensure_ascii=False, indent=4)
            else:
                print(res['result'], 'Failed to fetch ' + file_name)


def source_dataset(s3_bucket, new_s3_key):
    meta_res = requests.get(
        'https://delphi.cmu.edu/epidata/api.php?source=covidcast_meta').json()

    if meta_res['result'] == 1:
        for entry in meta_res['epidata']:
            query_and_save_json(entry['data_source'], entry['signal'], entry['time_type'],
                                entry['geo_type'], str(entry['min_time']), str(entry['max_time']))
        
        with open('/tmp/covidcast-meta.json', 'w', encoding='utf-8') as f:
            json.dump(
                meta_res['epidata'], f, ensure_ascii=False, indent=4)
       	
        s3 = boto3.client("s3")

        job_list = []

        for filename in os.listdir('/tmp'):
            print('saving ' + filename)
            job_list.append(
                {
                    'Bucket': s3_bucket,
                    'Key': new_s3_key + filename
                }
            )
            s3.upload_file('/tmp/' + filename, s3_bucket, new_s3_key + filename)

        return job_list
