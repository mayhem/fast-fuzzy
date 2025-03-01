import atexit
from multiprocessing import Process, Queue

from flask import Flask, request, jsonify
from werkzeug.exceptions import BadRequest

from search_index import MappingLookupSearch
from search_process import mapping_lookup_process
from fuzzy_index import FuzzyIndex


INDEX_DIR = "index"
NUM_SHARDS = 2
ARTIST_CONFIDENCE = .5

def create_shard_processes(ms):

    shards = []
    shard_index = {}
    for i in range(NUM_SHARDS):
        request_queue = Queue()
        response_queue = Queue()
        p = Process(target=mapping_lookup_process, args=(request_queue, response_queue, INDEX_DIR, NUM_SHARDS, i))
        p.start()
        shards.append({ "process" : p, "in_q": request_queue, "out_q": response_queue })
        for ch in ms.shards[i]["shard_ch"]:
            shard_index[ch] = i

    return shards, shard_index

def stop_shard_processes():

    # Send each worker a message to exit
    request = { "exit": True }
    for shard in shards:
        shard["in_q"].put(request)

    # Join each process and then join the queues
    for shard in shards:
        shard["process"].join()
        shard["in_q"].close()
        shard["in_q"].join_thread()
        shard["out_q"].close()
        shard["out_q"].join_thread()

ms = MappingLookupSearch(INDEX_DIR, NUM_SHARDS)
ms.split_shards()
shards, shard_index = create_shard_processes(ms)

artist_index = FuzzyIndex()
artist_index.load(INDEX_DIR)

app = Flask(__name__)

def cleanup():
    stop_shard_processes()

atexit.register(cleanup)

@app.route("/search")
def index():
    artist = request.args.get("a", "")
    release = request.args.get("rl", "")
    recording = request.args.get("rc", "")
    if not artist or not recording:
        raise BadRequest("a and rc must be given")

    encoded = FuzzyIndex.encode_string(artist)
    artists = artist_index.search(artist, min_confidence=ARTIST_CONFIDENCE)
    for a in artists:
        print(a)

    req = { "artist_ids": [ x["index"] for x in artists ],
            "artist_name": artist,
            "release_name": release,
            "recording_name": recording }

    shard = shard_index[encoded[0]]
    shards[shard]["in_q"].put(req)
    response = shards[shard]["out_q"].get()

    return jsonify(response)
