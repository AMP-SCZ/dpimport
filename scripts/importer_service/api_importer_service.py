import requests


class ImporterApiService:
    def __init__(
        self,
        credentials,
    ):
        self.headers = {"content-type": "application/json", **credentials}

    def routes(self, url, path):
        routes = {"metadata": url + "metadata", "day_data": url + "day"}

        return routes[path]

    def upsert_file(self, url, file_data):
        r = requests.post(
            url,
            headers=self.headers,
            data=file_data,
        )
        status = r.status_code
        if status != 200:
            response = r.json()["message"]
            print(response)
        else:
            response = r.json()["data"]
            print(response)
