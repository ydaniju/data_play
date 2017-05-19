require 'rest-client'
require 'base64'
require 'json'
require 'csv'

uaa_url = 'https://iiotquest-uaa-service.predix-uaa.run.aws-usw02-pr.ice.predix.io/oauth/token'
ts_url = 'https://time-series-store-predix.run.aws-usw02-pr.ice.predix.io/v1/datapoints'
zone_id = 'e897dc32-c491-4641-9c54-3f3bd75ca189'
token = Base64.encode64('timeseries_client_readonly:IM_SO_SECRET')

def doQuery(payload, ts_url, uaa_url, token, zone_id)
  headers = {
    'authorization' => 'Basic ' + token,
    'cache-control' => 'no-cache',
    'content-type' => 'application/x-www-form-urlencoded'
  }
  response = RestClient::Request.execute(method: :post, url: uaa_url,
                                         payload: 'grant_type=client_credentials',
                                         headers: headers)

  token = JSON.parse(response)['access_token']
  headers = {
    'authorization' => 'Bearer ' + token,
    'predix-zone-id' => zone_id,
    'content-type' => 'application/json',
    'cache-control' => 'no-cache'
  }
  puts payload
  response = RestClient::Request.execute(method: :post, url: ts_url,
                                         payload: payload,
                                         headers: headers)

  data = JSON.parse(response)['tags'][0]['results'][0]['values']
  data
end

def last_payload(tag)
  hash = {
    start: '1y-ago',
    tags: [
      { name: tag, order: 'desc', limit: 1 }
    ]
  }

  JSON.generate(hash)
end

def first_payload(tag)
  hash = {
    start: '1y-ago',
    tags: [
      { name: tag, order: 'asc', limit: 1 }
    ]
  }

  JSON.generate(hash)
end

tags = %w(
  WT001_ACTIVE_POWER
  WT001_AMBIENT_TEMP
  WT001_HYDRAULIC_OIL_TEMP
  WT001_PHASE_CURRENT_A
  WT001_PHASE_CURRENT_B
  WT001_PHASE_CURRENT_C
  WT001_PHASE_VOLTAGE_A
  WT001_PHASE_VOLTAGE_B
  WT001_PHASE_VOLTAGE_C
  WT001_REACTIVE_POWER
  WT001_TURBINE_STATUS
  WT001_WIND_DIRECTION
  WT001_WIND_SPEED
)

hash = Hash.new([])
tags.each do |tag|
  payload_first = first_payload(tag)
  first_point = doQuery(payload_first, ts_url, uaa_url, token, zone_id)
  start_date = first_point[0][0]
  payload = {
    cache_time:  0,
    tags: [{ name: tag, order: 'asc' }],
    start: start_date
  }

  series = doQuery(JSON.generate(payload), ts_url, uaa_url, token, zone_id)
  series.each_with_index do |e, _i|
    hash[e[0]] += [e[1]]
  end
end

headers = tags.unshift('timestamp')
arr = []
a = 0
hash.each do |k, v|
  arr[a] = [k, v].flatten
  a += 1
end

CSV.open('data.csv', 'wb', write_headers: true, headers: headers) do |csv|
  arr.each do |elem|
    csv << elem
  end
end
