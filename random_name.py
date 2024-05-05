#randomname_api will be data producer for kafka so this produced data will be pushed into kafka topic
import requests

def random_api(api_url):
    header = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36',
        'Content-Type': 'application/json',
    }

    resp = requests.get(api_url, headers=header)
    if resp.status_code ==200:
        resp = resp.json()
        kafka_item = {'name': resp['results'][0]['name']['first'] + " " + resp['results'][0]['name']['last'],
                'gender': resp['results'][0]['gender'],
                'country': resp['results'][0]['location']['country'],
                'email': resp['results'][0]['email'],
                'phone': resp['results'][0]['cell']}
        print(kafka_item)
        return kafka_item

