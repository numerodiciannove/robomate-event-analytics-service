# import requests
#
# url = "http://127.0.0.1:8000/api/events"
# headers = {"Content-Type": "application/json"}
#
# with open("events_sample.json", "rb") as f:
#     response = requests.post(url, headers=headers, data=f)
#
# print("Status Code:", response.status_code)
# print("Response:", response.text)


import requests
import json

url = "http://localhost:5066/api/events"
headers = {"Content-Type": "application/json"}

data = [
    {"event_id": "11111111-1111-1111-1111-111111111111", "occurred_at": "2025-10-25T12:00:00Z",
     "user_id": 1, "event_type": "login", "properties": {}}
]

for i in range(20):
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print(i+1, response.status_code, response.text)
