import time
import json
from random import randint
import urllib.parse

from locust import HttpUser, task, between

DATA_FILE = "../../typesense_queries.txt"
docs = []
with open(DATA_FILE, "r") as f:
    for line in f.readlines():
        docs.append(json.loads(line))

class FuzzySearch(HttpUser):
    wait_time = 1

    @task()
    def do_search(self):
        global docs
        i = randint(0, len(docs) - 1)
        artist = urllib.parse.quote(docs[i]["artist"])
        recording = urllib.parse.quote(docs[i]["recording"])
        print(f'/search?a={artist}&r={recording}')
        self.client.get(f'/search?a={artist}&r={recording}', name="/search")
