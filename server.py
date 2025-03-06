import atexit
from multiprocessing import Process, Queue
from multiprocessing.queues import Empty

from flask import Flask, request, jsonify, render_template, redirect
from werkzeug.exceptions import BadRequest, ServiceUnavailable, NotFound
from lb_matching_tools.cleaner import MetadataCleaner

from search_index import MappingLookupSearch
from search_process import mapping_lookup_process
from fuzzy_index import FuzzyIndex


INDEX_DIR = "index"
NUM_SHARDS = 8
SHORT_ARTIST_LENGTH = 5
SHORT_ARTIST_CONFIDENCE = .5
NORMAL_ARTIST_CONFIDENCE = .7

# If the search hit is less than this, clean metadata and search those too!
CLEANER_CONFIDENCE = .9

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

def mapping_search(artist, release, recording):

    mc = MetadataCleaner()
    encoded_artist = artist_index.encode_string(artist)
    shard_ch = None

    # Is this a normal (not stupid) artist?
    if encoded_artist:
        if len(encoded_artist) <= SHORT_ARTIST_LENGTH:
            confidence = SHORT_ARTIST_CONFIDENCE
        else:
            confidence = NORMAL_ARTIST_CONFIDENCE

        # Do a normal artist search
        artists = artist_index.search(encoded_artist, min_confidence=confidence, debug=True)
        try:
            max_confidence = max([ a["confidence"] for a in artists ])
        except ValueError:
            max_confidence = 0.0
        print(artists)

        if max_confidence <= CLEANER_CONFIDENCE:
            cleaned_artist = artist_index.encode_string(mc.clean_artist(artist))
            if cleaned_artist != encoded_artist:
                artists.extend(artist_index.search(cleaned_artist, min_confidence=confidence, debug=True))

        if not artists:
            raise NotFound("Artist '%s' was not found." % artist)

        artists = sorted(artists, key=lambda a: a["confidence"], reverse=True)
        shard_ch = artists[0]["shard_ch"]

        # Collect the artist ids
        ids = []
        for a in artists:
            if a["shard_ch"] == shard_ch:
                ids.append(a["id"])

        print("ids: ", ids)
    else:
        # If the name contains no word characters (stoopid), search the stupid artists and send them to the stooopid shard
        if stupid_artist_index:
            encoded = FuzzyIndex.encode_string_for_stupid_artists(artist)
            artists = stupid_artist_index.search(encoded, min_confidence=NORMAL_ARTIST_CONFIDENCE)
            shard_ch = "$"
            ids = [ a["id"] for a in artists ]
        else:
            return jsonify({})

    if not ids:
        raise NotFound("Artist '%s' was not found." % artist)

    print("on shard '%s' search on: " % shard_ch)
    for a in artists:
        print("  %-30s %10d %.3f" % (a["text"][:30], a["id"], a["confidence"]))

    # Make the search request
    req = { "artist_ids": ids,
            "artist_name": artist,
            "release_name": release,
            "recording_name": recording }
    try:
        shard = shard_index[shard_ch]
    except KeyError:
        raise BadRequest("Shard not availble for char '%s'" % encoded)

    shards[shard]["in_q"].put(req)
    try:
        response = shards[shard]["out_q"].get(timeout=SEARCH_TIMEOUT)
    except Empty:
        raise ServiceUnavailable("Search timed out.")

    return response

@app.route("/")
def index():
    return redirect("/search")

@app.route("/search", methods=["GET"])
def search():
    return render_template("index.html")

@app.route("/search", methods=["POST"])
def search_post():
    artist = request.form.get("artist", "")
    release = request.form.get("release", "")
    recording = request.form.get("recording", "")
    if not artist or not recording:
        raise BadRequest("artist and recording must be given")

    return render_template("index.html", results=mapping_search(artist, release, recording),
                                         artist=artist,
                                         release=release,
                                         recording=recording)

@app.route("/1/search")
def api_search():
    artist = request.args.get("a", "")
    release = request.args.get("rl", "")
    recording = request.args.get("rc", "")
    if not artist or not recording:
        raise BadRequest("a and rc must be given")

    return jsonify(mapping_search(artist, release, recording))
