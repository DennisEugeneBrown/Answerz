import requests
from pprint import pprint


def interpret(text, luis_app_id, luis_subscription_key, staging="true"):
    url = "https://westus.api.cognitive.microsoft.com/luis/prediction/v3.0/apps/{}/slots/staging/predict?subscription-key={}&verbose=true&show-all-intents=true&log=true&query={}".format(
        luis_app_id, luis_subscription_key, text)
    r = requests.get(url=url)
    data = r.json()
    pprint(data)
    return data
