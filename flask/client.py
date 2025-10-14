import requests
import json

url = 'http://localhost:5000/api/data'
data = {
    'name': 'John Doe',
    'age': 30,
    'city': 'New York'
}

headers = {'Content-Type': 'application/json'}

response = requests.post(url, data=json.dumps(data), headers=headers)

print("Status Code:", response.status_code)
print("Response:", response.json())
