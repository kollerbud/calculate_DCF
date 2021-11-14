import requests
import json
from api_keys import BING_MAPS_API_KEYS

from requests.models import Response

api_key = BING_MAPS_API_KEYS.api_key

# REST api format
# http://dev.virtualearth.net/REST/v1/Routes/{travelMode}?wayPoint.1={wayPoint1}&viaWaypoint.2={viaWaypoint2}&waypoint.3={waypoint3}&wayPoint.n={waypointN}&heading={heading}&optimize={optimize}&avoid={avoid}&distanceBeforeFirstTurn={distanceBeforeFirstTurn}&routeAttributes={routeAttributes}&timeType={timeType}&dateTime={dateTime}&maxSolutions={maxSolutions}&tolerances={tolerances}&distanceUnit={distanceUnit}&key={BingMapsKey}
WayPoint_1 = [41.907613, -87.669300]
WayPoint_2 = [41.893382, -87.621999]

url = f'http://dev.virtualearth.net/REST/V1/Routes/Transit?wp.0={WayPoint_1[0]},{WayPoint_1[1]}&wp.1={WayPoint_2[0]},{WayPoint_2[1]}&timeType=Departure&dateTime=8:00:00AM&output=json&key=Ai-OQTu-oIUMgzFhilQrKS-uYcDPMKbptqRn4XWQy9wojDw_cDGkS4pO7NHR6d7v'

req = requests.get(url=url)
print(req.status_code)

#with open('bing_map_response.json') as f:
#    data = json.load(f)
