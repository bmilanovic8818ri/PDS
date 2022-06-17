import requests

url = "http://192.168.49.1:8081/users"

payload="{\n    \"ime\": \"Bogdan\",\n    \"prezime\": \"Milanovic\",\n    \"smer\": \"RI\",\n    \"predmeti\": [\n        {\n            \"ime\": \"Algoritmi\",\n            \"espb\": \"8\"\n        }\n    ]\n}"
headers = {
  'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJOZWtpQGdtYWlsLmNvbSIsInJvbGUiOiJhZG1pbiIsImV4cCI6MTYzMTAyNzUwNywiaWF0IjoxNjMwOTQxMTA3fQ.1-KalobWwi9TYfcwvy40ya-EKWrSAO-5HCb7d5Y3tFU',
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
