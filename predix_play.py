import pandas as pd
import requests
import json
import base64
# from bokeh.charts import TimeSeries, output_file, show
# output_file("test.html")

sess = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries = 20)
sess.mount('https://', adapter)


uaaUrl = "https://iiotquest-uaa-service.predix-uaa.run.aws-usw02-pr.ice.predix.io/oauth/token"
tsUrl = "https://time-series-store-predix.run.aws-usw02-pr.ice.predix.io/v1/datapoints"
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
    # series['timestamp'] = pd.to_datetime(series['timestamp'], unit='ms')
    return series

def last_payload(tag):
  return "{\n  \"start\": \"1y-ago\",\n  \"tags\": [\n    {\n      \"name\": \"%s\",\n      \"order\": \"desc\",\n      \"limit\": 1\n    }\n  ]\n}" % tag

def first_payload(tag):
  return "{\n  \"start\": \"1y-ago\",\n  \"tags\": [\n    {\n      \"name\": \"%s\",\n      \"order\": \"asc\",\n      \"limit\": 1\n    }\n  ]\n}" % tag

tags = [
  "WT001_ACTIVE_POWER",
  "WT001_AMBIENT_TEMP",
  "WT001_HYDRAULIC_OIL_TEMP",
  "WT001_PHASE_CURRENT_A",
  "WT001_PHASE_CURRENT_B",
  "WT001_PHASE_CURRENT_C",
  "WT001_PHASE_VOLTAGE_A",
  "WT001_PHASE_VOLTAGE_B",
  "WT001_PHASE_VOLTAGE_C",
  "WT001_REACTIVE_POWER",
  "WT001_TURBINE_STATUS",
  "WT001_WIND_DIRECTION",
  "WT001_WIND_SPEED"
]

pdArray = []
for tag in tags:

  payload_first = first_payload(tag)
  payload_last = last_payload(tag)
  firstPoint = doQuery(payload_first, tsUrl, uaaUrl, token, zoneId)
  startDate =  pd.Timestamp(firstPoint['timestamp'][0])
  startDateOrigin = startDate = int(startDate.strftime("%s")) * 1000

  payload = { 
    'cache_time': 0,
    'tags': [{'name': tag, 'order': 'asc'}],
    'start': startDate
  }
    
  series = doQuery(json.dumps(payload), tsUrl, uaaUrl, token, zoneId)
  times = series.get('timestamp')
  values = series.get('values')
  for a in values:
    print a

  pdArray.append(series)

fullseries = pd.concat(pdArray)

fullseries.to_csv('power_series.csv', sep='\t', encoding='utf-8')
