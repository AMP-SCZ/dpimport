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
        try:
            response = r.json()
            print(response)
        except Exception as e:
            print(status)
            print(f"Error: {e}")
            print(r)
            pass

    def refresh_metadata_collection(self, url):
        r = requests.delete(url, headers=self.headers)
        status = r.status_code

        if status != 200:
            print("There was an error.", r.json()["message"])
        else:
            response = r.json()["data"]
            print(response)
