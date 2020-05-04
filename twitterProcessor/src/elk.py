import requests

res = requests.post('http://localhost:9200/index1/_doc', json={'yanki': 'yanki'})
print(res.content)