import time
import json
from random import randint
import urllib.parse

from locust import HttpUser, task, constant

DATA_FILE = "../../typesense_queries.txt"
docs = []
with open(DATA_FILE, "r") as f:
    for line in f.readlines():
        docs.append(json.loads(line))

class FuzzySearch(HttpUser):
    wait_time = constant(1)

    @task()
    def do_search(self):
        global docs
        i = randint(0, len(docs) - 1)
        artist = urllib.parse.quote(docs[i]["artist"])
        recording = urllib.parse.quote(docs[i]["recording"])
        with self.client.get(f'/1/search?a={artist}&rc={recording}', name="/search", catch_response=True) as response:
            if response.status_code in (200, 404):
                response.success()
            else:
                response.failure("error code: %d" % response.status_code)
