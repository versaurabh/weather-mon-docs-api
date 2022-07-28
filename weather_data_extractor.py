import requests
import json
from datetime import datetime, timedelta

ASTRA_DB_ID='<<ASTRA_DB_ID>>'
ASTRA_DB_REGION='us-east1'
ASTRA_DB_KEYSPACE='weathermon'
ASTRA_DB_APPLICATION_TOKEN='<<ASTRA_DB_APPLICATION_TOKEN>>'
headers = {"X-Cassandra-Token":"{ASTRA_DB_APPLICATION_TOKEN}".format(ASTRA_DB_APPLICATION_TOKEN=ASTRA_DB_APPLICATION_TOKEN)}
collection = 'weatherapi'
WEATHER_API_KEY='<<WEATHER_API_KEY>>'

cities = ['Seattle', 'Chicago', 'Boston']
startDate = '2022-10-25'
numDays =10 

# cleanup weatherapi collection
delUrl = 'https://{ASTRA_DB_ID}-{ASTRA_DB_REGION}.apps.astra-dev.datastax.com/api/rest/v2/namespaces/{ASTRA_DB_KEYSPACE}/collections/{collection}'.format(ASTRA_DB_ID=ASTRA_DB_ID,ASTRA_DB_REGION=ASTRA_DB_REGION,ASTRA_DB_KEYSPACE=ASTRA_DB_KEYSPACE,collection=collection)
delReq = requests.delete(delUrl, headers=headers)

if (delReq.status_code == 204):
	print("Collection {collection} cleanedup!".format(collection=collection))
elif (delReq.status_code == 404):
	print("Collection {collection} doesnt exist!".format(collection=collection))
else:
	print("Err cleaning {collection} : code {code}".format(collection=collection))
	exit(-1)

for x in cities:
	for i in range(numDays):
		dt = datetime.strptime(startDate, '%Y-%m-%d') + timedelta(i)
		dd = datetime.strftime(dt, '%Y-%m-%d')
		day = dt.strftime('%A')
		week = dt.strftime('%V')

		res = requests.get('https://api.weatherapi.com/v1/history.json?key={WEATHER_API_KEY}&q={x}&dt={d}'.format(WEATHER_API_KEY=WEATHER_API_KEY, x=x, d=dd))
		if res.status_code == 200:
			j = res.json()
			c = {'city': x, 'week': week, 'location': j['location'], 'forecast': [ {'day': day, 'date': dd,'stats': j['forecast']['forecastday'][0] }]}

			uploadUrl = 'https://{ASTRA_DB_ID}-{ASTRA_DB_REGION}.apps.astra-dev.datastax.com/api/rest/v2/namespaces/{ASTRA_DB_KEYSPACE}/collections/weatherapi/{city}'.format(ASTRA_DB_ID=ASTRA_DB_ID,ASTRA_DB_REGION=ASTRA_DB_REGION,ASTRA_DB_KEYSPACE=ASTRA_DB_KEYSPACE,city=x + "_" + week)

			# check if city_week exists
			print(x + " : " + dd + " : " + uploadUrl)
			if (requests.get(uploadUrl, headers=headers).status_code == 200):
				patchUrl = 'https://{ASTRA_DB_ID}-{ASTRA_DB_REGION}.apps.astra-dev.datastax.com/api/rest/v2/namespaces/{ASTRA_DB_KEYSPACE}/collections/weatherapi/{city}/forecast/function'.format(ASTRA_DB_ID=ASTRA_DB_ID,ASTRA_DB_REGION=ASTRA_DB_REGION,ASTRA_DB_KEYSPACE=ASTRA_DB_KEYSPACE,city=x + "_" + week)
				print(patchUrl)
				payload = {'operation': '$push', 'value': {'day': day, 'date': dd,'stats': j['forecast']['forecastday'][0] }}
				p = requests.post(patchUrl, json = payload, headers=headers)
				print("PATCH " + str(p.status_code))
			else:
				p = requests.put(uploadUrl, data = json.dumps(c), headers=headers)
				print("PUT " + str(p.status_code))

			


