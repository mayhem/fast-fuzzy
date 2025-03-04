import atexit
from multiprocessing import Process, Queue

from flask import Flask, request, jsonify, render_template
from werkzeug.exceptions import BadRequest, ServiceUnavailable

from search_index import MappingLookupSearch
from search_process import mapping_lookup_process
from fuzzy_index import FuzzyIndex


INDEX_DIR = "index"
NUM_SHARDS = 8
ARTIST_CONFIDENCE = .5
SEARCH_TIMEOUT = 1  # in seconds

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

artist_index = FuzzyIndex("artist_index")
artist_index.load(INDEX_DIR)

stupid_artist_index = FuzzyIndex("stupid_artist_index")
if not stupid_artist_index.load(INDEX_DIR):
    stupid_artist_index = None

app = Flask(__name__, template_folder = "templates")

def cleanup():
    stop_shard_processes()

atexit.register(cleanup)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/search")
def search():
    artist = request.args.get("a", "")
    release = request.args.get("rl", "")
    recording = request.args.get("rc", "")
    if not artist or not recording:
        raise BadRequest("a and rc must be given")

    # Do a normal artist search
    encoded = FuzzyIndex.encode_string(artist)
    artists = artist_index.search(encoded, min_confidence=ARTIST_CONFIDENCE)

    # if we found something, save the shard
    if encoded:
        shard_ch = encoded[0]
    else:
        # If we didn't find anything, search the stupid artists and send them to the stooopid shard
        if stupid_artist_index:
            encoded = FuzzyIndex.encode_string_for_stupid_artists(artist)
            artists.extend(stupid_artist_index.search(encoded, min_confidence=ARTIST_CONFIDENCE))
            shard_ch = "$"
        else:
            return jsonify({})

    # Collect the artist ids
    ids = []
    for a in artists:
        if a["text"][0] == encoded[0]:
            ids.append(a["index"])

    # Make the search request
    req = { "artist_ids": ids,
            "artist_name": artist,
            "release_name": release,
            "recording_name": recording }
    try:
        print("shard ch: '%s'" % shard_ch)
        shard = shard_index[shard_ch]
    except KeyError:
        raise BadRequest("Shard not availble for char '%s'" % encoded)

    shards[shard]["in_q"].put(req)
    response = shards[shard]["out_q"].get(timeout=SEARCH_TIMEOUT)
    if response is None:
        raise ServiceUnavailable("Search timed out.")

    return jsonify(response)
