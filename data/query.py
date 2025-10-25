import requests
import time

url = "http://localhost:5066/api/events"
headers = {"Content-Type": "application/json"}

with open("events_100k.json", "rb") as f:
    start_time = time.time()
    response = requests.post(url, headers=headers, data=f)
    end_time = time.time()

elapsed_time = end_time - start_time

print("Status Code:", response.status_code)
print("Response:", response.text)
print(f"Request processing time: {elapsed_time:.2f} seconds")
