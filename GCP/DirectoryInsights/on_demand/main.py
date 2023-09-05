import croniter
import datetime
import json
import os
import requests
import time
from google.cloud import storage


def json_array_dump(data):
    return '['+ json.dumps(data) + ']'

def jc_directory_insights(start_arg, end_arg):
    try:
        jc_api_key  = os.environ['jc_api_key']
        jc_org_id = os.environ['jc_org_id']
        service =  os.environ['service']
        bucket_name = os.environ['bucket_name']
        
        start_date_str = start_arg
        # override for local testing
        if 'start_date' in os.environ:
            start_date_str = os.environ['start_date']
        
        end_date_str = end_arg
        # override for local testing
        if 'end_date' in os.environ:
            end_date_str = os.environ['end_date']

    except KeyError as e:
        raise Exception(e)
    
    try:
        start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%SZ")
        end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as e:
        raise Exception(e)
    
    _current = start_date
    print(f'Fetching all JumpCloud logs from {start_date} to {end_date}')
    while _current < end_date:
        _start = _current
        _current += datetime.timedelta(hours=1)
        _end = _current

        if _end > end_date:
            _end = end_date

        start = _start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end = _end.strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f'Fetching JumpCloud logs from {start} to {end}')
        
        available_services = ['directory', 'radius', 'sso', 'systems', 'ldap', 'mdm', 'all']
        service_list = ((service.replace(" ", "")).lower()).split(",")
        for service in service_list:
            if service not in available_services:
                raise Exception(f"Unknown service: {service}")
        if 'all' in service_list and len(service_list) > 1:
            raise Exception(f"Error - Service List contains 'all' and additional services : {service_list}")
        final_data = ""
    
        if len(service_list) > 1:
            for service in service_list:
                print (f' service: {service},\n start-date: {start},\n end-date: {end}')
            for service in service_list:
                print (f' service: {service},\n start-date: {start},\n end-date: {end}')
            
        for service in service_list:
            url = "https://api.jumpcloud.com/insights/directory/v1/events"
            body = {
                'service': [f"{service}"],
                'start_time': start,
                'end_time': end,
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
        
        final_data = '\n'.join(map(json_array_dump,data))
    
        print(final_data)

        if len(final_data) == 0:
            return
        else:
            outfile_name = "jc_directoryinsights_" + start + "_" + end + ".json"
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(outfile_name)
            blob.upload_from_string(
                data=json.dumps(final_data),
                content_type='application/json'
            )
            print(f'Uploaded {outfile_name}')

# Http function for GC Functions
def run_di(httpRequest):
    requests_args = httpRequest.args
    payload = httpRequest.get_json()

    if requests_args and "message" in requests_args:
        message = requests_args["message"]
    else:
        jc_directory_insights(payload.get("start"), payload.get("end"))
        message = 'DI successfully ran'
    return message