import requests

url = "http://127.0.0.1:8000/api/events"
headers = {"Content-Type": "application/json"}

with open("events_sample.json", "rb") as f:
    response = requests.post(url, headers=headers, data=f)

print("Status Code:", response.status_code)
print("Response:", response.text)
