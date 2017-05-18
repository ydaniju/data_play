import pandas as pd
import requests
import json
import base64
from bokeh.charts import TimeSeries, output_file, show
output_file("test.html")

sess = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries = 20)
sess.mount('https://', adapter)


uaaUrl = "https://iiotquest-uaa-service.predix-uaa.run.aws-usw02-pr.ice.predix.io/oauth/token"
tsUrl = "https://time-series-store-predix.run.aws-usw02-pr.ice.predix.io/v1/datapoints"
payload_last = "{\n  \"start\": \"1y-ago\",\n  \"tags\": [\n    {\n      \"name\": \"WT001_ACTIVE_POWER\",\n      \"order\": \"desc\",\n      \"limit\": 1\n    }\n  ]\n}"
payload_first = "{\n  \"start\": \"1y-ago\",\n  \"tags\": [\n    {\n      \"name\": \"WT001_ACTIVE_POWER\",\n      \"order\": \"asc\",\n      \"limit\": 1\n    }\n  ]\n}"
zoneId = "e897dc32-c491-4641-9c54-3f3bd75ca189"
token = base64.b64encode('timeseries_client_readonly:IM_SO_SECRET')

def doQuery(payload, tsUrl, uaaUrl, token, zoneId):
    headers = {
        'authorization': "Basic " + token,
        'cache-control': "no-cache",
        'content-type': "application/x-www-form-urlencoded"
    }
    response = sess.request('POST', uaaUrl, data="grant_type=client_credentials", headers=headers)
    token = json.loads(response.text)['access_token']
    headers = {
        'authorization': "Bearer " + token,
        'predix-zone-id': zoneId,
        'content-type': "application/json",
        'cache-control': "no-cache"
    }
    response = sess.request("POST", tsUrl, data=payload, headers=headers)
    data = json.loads(response.text)['tags'][0]['results'][0]['values']
    column_labels = ['timestamp', 'values', 'quality']
    series = pd.DataFrame(data, columns=column_labels)
    series['timestamp'] = pd.to_datetime(series['timestamp'], unit='ms')
    return series

firstPoint = doQuery(payload_first, tsUrl, uaaUrl, token, zoneId)
startDate =  pd.Timestamp(firstPoint['timestamp'][0])
startDateOrigin = startDate = int(startDate.strftime("%s")) * 1000

lastPoint = doQuery(payload_last, tsUrl, uaaUrl, token, zoneId)
endDate =  pd.Timestamp(lastPoint['timestamp'][0])
endDate = int(endDate.strftime("%s")) * 1000
pdArray = []
while (startDate < endDate ):
    payload = { 
      'cache_time': 0,
      'tags': [{'name': 'WT001_ACTIVE_POWER', 'order': 'asc'}],
      'start': startDate, 'end': startDate + 10000000
    }
    
    startDate = startDate + 100000000
    series = doQuery(json.dumps(payload), tsUrl, uaaUrl, token, zoneId)
    pdArray.append(series)

fullseries = pd.concat(pdArray)


data = dict(values = fullseries['values'], Date=fullseries['timestamp'])
p = TimeSeries(data, x='Date', y='values')
show(p)