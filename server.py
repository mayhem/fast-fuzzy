from multiprocessing import Process, Queue

from flask import Flask, request, jsonify
from werkzeug.exceptions import BadRequest

from search_index import MappingLookupSearch


INDEX_DIR = "index"
NUM_SHARDS = 8

request_queue = Queue()
response_queue = Queue()

ms = MappingLookupSearch(index_dir, NUM_SHARDS)
ms.load_artist_index()
shards = ms.create_shards()


app = Flask(__name__)

@app.route("/search")
def index():
    artist = request.args.get("artist", "")
    release = request.args.get("release", "")
    recording = request.args.get("recording", "")
    if not artist or not recording:
        raise BadRequest("a and r must be given")

    return jsonify(mi.search(artist, release, recording))
