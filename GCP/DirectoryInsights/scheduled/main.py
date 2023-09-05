import croniter
import datetime
import json
import os
import re
import requests
from google.cloud import storage

def json_array_dump(data):
    return '['+ json.dumps(data) + ']'

def jc_directory_insights():
    # Constant variables
    pattern = r'jc_directoryinsights_(\S*)_(\S*).json'

    # Cloud function variables
    try:
        jc_api_key = os.environ['jc_api_key']
        jc_org_id = os.environ['jc_org_id']
        cron_schedule = os.environ['cron_schedule']
        service =  os.environ['service']
        bucket_name = os.environ['bucket_name']

    except KeyError as e:
        raise Exception(e)

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='jc_directoryinsights')
    blobs_list = list(blobs)

    # Move old <prefix>_<start_timestamp>_<end_timestamp>.json to <start_date>/<file_json>.json 
    # and keep last 100 as is in order to improve grouping and file inspection
    if len(blobs_list) == 0:
        print("Info: Initial Run of cloud function")
    elif len(blobs_list) > 100:
        # move old files to subfolders except last 100
        for blob in blobs_list[:-100] :
            # Match the pattern
            match = re.match(pattern, blob.name)
            # Check if there is a match
            if match:
                start_date = match.group(1).split('T')[0]
                new_name = start_date + "/" + blob.name
                print(f"[ Info ] Archive {blob.name}")
                # Copy the blob to the new location
                new_blob = bucket.copy_blob(blob, bucket, new_name)
                # Delete the original blob
                blob.delete()
            else:
                print(f"[ Error ] Bad name {blob.name}")
                bad_name = "bad" + "_" + blob.name
                new_blob = bucket.copy_blob(blob, bucket, bad_name)
                # Delete the original blob
                blob.delete()
    
    date_now = datetime.datetime.utcnow()
    now = date_now.replace(second=0, microsecond=0) 
    cron = croniter.croniter(cron_schedule, now)
    end_date = now.isoformat("T") + "Z"
    start_date = ""  
   
    if len(blobs_list) == 0:
        start_dt = cron.get_prev(datetime.datetime)
        start_date = start_dt.isoformat("T") + "Z"
    else: 
        # Start from the last ingested date
        last_ingested_blob = blobs_list[-1]
        match = re.match(pattern, last_ingested_blob.name)
        start_date = match.group(2)
        print(f"[ Info ] Last ingested timestamp {start_date}")
          
    available_services = ['directory', 'radius', 'sso', 'systems', 'ldap', 'mdm', 'all']
    service_list = ((service.replace(" ", "")).lower()).split(",")
    for service in service_list:
        if service not in available_services:
            raise Exception(f"Unknown service: {service}")
    if 'all' in service_list and len(service_list) > 1:
        raise Exception(f"Error - Service List contains 'all' and additional services : {service_list}")
    final_data = ""
       
    try:
        request_start_date = datetime.datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
        request_end_date = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")
        _end_dt = None

        # if end_date_str is "2023-09-10T16:00:00Z" the end is "2023-09-10T16:00:00Z"
        # if end_date_str is "2023-09-10T16:00:01Z" the end is "2023-09-10T16:00:00Z"
        # if end_date_str is "2023-09-10T15:59:59Z" the end is "2023-09-10T15:45:00Z"
        cron = croniter.croniter(cron_schedule, request_end_date)
        _dt = cron.get_prev(datetime.datetime)

        _next_cron_dt = croniter.croniter(cron_schedule, _dt)
        _future_dt = _next_cron_dt.get_next(datetime.datetime)

        if _future_dt <= request_end_date:
            _end_dt  = _future_dt
        else:
            _end_dt  = _dt

        # update request_end_date
        request_end_date = _end_dt

        if request_start_date != _end_dt:
            print("[ Info ] Requesting events from", request_start_date.strftime("%Y-%m-%dT%H:%M:%SZ"), 
                "to", request_end_date.strftime("%Y-%m-%dT%H:%M:%SZ"))

    except ValueError as e:
        raise Exception(e)
    
    _current = request_start_date
    while _current < request_end_date:
        _start = _current
        _current += datetime.timedelta(minutes=15)
        _end = _current

        if _end > request_end_date:
            _end = request_end_date

        period_request_start = _start.strftime("%Y-%m-%dT%H:%M:%SZ")
        period_request_end = _end.strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f'[ Info ] Fetching JumpCloud logs from {period_request_start} to {period_request_end}')

        for service in service_list:
            url = "https://api.jumpcloud.com/insights/directory/v1/events"
            body = {
                'service': [f"{service}"],
                'start_time': period_request_start,
                'end_time': period_request_end,
                "limit": 10000
            }
            headers = {
                'x-api-key': jc_api_key,
                'content-type': "application/json",
                'user-agent': 'JumpCloud_GCPServerless.DirectoryInsights/0.0.1'
            }
            if jc_org_id != '':
                headers['x-org-id'] = jc_org_id
            response = requests.post(url, json=body, headers=headers)

            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                raise Exception(e)

            response_body = json.loads(response.text)
            data = response_body
            while response.headers["X-Result-Count"] >= response.headers["X-Limit"]:
                body["search_after"] = json.loads(response.headers["X-Search_After"])
                response = requests.post(url, json=body, headers=headers)
                try:
                    response.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    raise Exception(e)

                response_body = json.loads(response.text)
                data = data + response_body
            # for d in data:
            #     final_data += json.dumps(d)
            #     final_data += ',\n'
            #final_data = '\n'.join(map(json.dumps,data))
            final_data = '\n'.join(map(json_array_dump,data))

        if len(final_data) == 0:
            pass
        else:
            outfile_name = "jc_directoryinsights_" + period_request_start + "_" + period_request_end + ".json"
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(outfile_name)
            blob.upload_from_string(
                data=final_data,
                content_type='application/json'
            )
            # data=json.dumps(final_data),
            print(f'[ Info ] Uploaded {outfile_name}')

# Http function for GC Functions
def run_di(httpRequest):
    requests_args = httpRequest.args

    if requests_args and "message" in requests_args:
        message = requests_args["message"]
    else:
        jc_directory_insights()
        message = 'ok'
    return message
