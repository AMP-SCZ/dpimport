import requests


def routes(url, path):
    routes = {"metadata": url + "metadata", "day_data": url + "day"}

    return routes[path]


def upsert_file(url, file_data):
    headers = {"content-type": "application/json"}
    r = requests.post(
        url,
        headers=headers,
        data=file_data,
    )
    status = r.status_code
    if status != 200:
        response = r.json()["message"]
        print(response)
    else:
        response = r.json()["data"]
        print(response)
