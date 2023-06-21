# +
import requests
HEADERS = {

    "Accept": 'application/json',

}

BASE_URL = "https://data.brreg.no/enhetsregisteret/api/"
def get_json(url, params={}):

    url = BASE_URL + url
    req = requests.Request("GET", url=url, headers=HEADERS, params=params)

    #print(req.url, req.headers, req.params)
    print("Full URL, check during testing:", req.prepare().url)

    response = requests.Session().send(req.prepare())

    response.raise_for_status()

    # print(response.text)
    return response.json()

get_json("enheter")["_embedded"]

# -


