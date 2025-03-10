import atexit
from time import monotonic, sleep
from multiprocessing import Process, Queue
from multiprocessing.queues import Empty
import os
from uuid import uuid4

from flask import Flask, request, jsonify, render_template, redirect
from werkzeug.exceptions import BadRequest, ServiceUnavailable, NotFound, InternalServerError
from lb_matching_tools.cleaner import MetadataCleaner
from playhouse.shortcuts import model_to_dict

from database import open_db, Mapping
from search_index import MappingLookupSearch
from search_process import mapping_lookup_process
from fuzzy_index import FuzzyIndex


INDEX_DIR = "index"
NUM_SHARDS = 16 
SHORT_ARTIST_LENGTH = 5
SHORT_ARTIST_CONFIDENCE = .5
NORMAL_ARTIST_CONFIDENCE = .7

# If the search hit is less than this, clean metadata and search those too!
CLEANER_CONFIDENCE = .9

SEARCH_TIMEOUT = 10 # in seconds

def create_shard_processes(ms):

    shards = []
    for i in range(NUM_SHARDS):
        request_queue = Queue()
        response_queue = Queue()
        p = Process(target=mapping_lookup_process, args=(request_queue, response_queue, INDEX_DIR, NUM_SHARDS, i))
        p.start()
        shards.append({ "process" : p, "in_q": request_queue, "out_q": response_queue })

    return shards

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
shards = create_shard_processes(ms)

artist_index = FuzzyIndex("artist_index")
artist_index.load(INDEX_DIR)

stupid_artist_index = FuzzyIndex("stupid_artist_index")
if not stupid_artist_index.load(INDEX_DIR):
    stupid_artist_index = None

db_file = os.path.join(INDEX_DIR, "mapping.db")
app = Flask(__name__, template_folder = "templates")

def cleanup():
    stop_shard_processes()

atexit.register(cleanup)

def mapping_search(artist, release, recording):

    mc = MetadataCleaner()
    encoded_artist = artist_index.encode_string(artist)
    shard_ch = None
    
    open_db(db_file)

    # Is this a normal (not stupid) artist?
    if encoded_artist:
        if len(encoded_artist) <= SHORT_ARTIST_LENGTH:
            confidence = SHORT_ARTIST_CONFIDENCE
        else:
            confidence = NORMAL_ARTIST_CONFIDENCE

        # Do a normal artist search
        artists = artist_index.search(encoded_artist, min_confidence=confidence)
        try:
            max_confidence = max([ a["confidence"] for a in artists ])
        except ValueError:
            max_confidence = 0.0

        if max_confidence <= CLEANER_CONFIDENCE:
            cleaned_artist = artist_index.encode_string(mc.clean_artist(artist))
            if cleaned_artist != encoded_artist:
                artists.extend(artist_index.search(cleaned_artist, min_confidence=confidence))

        if not artists:
            raise NotFound("Artist '%s' was not found." % artist)

        artists = sorted(artists, key=lambda a: a["confidence"], reverse=True)
        shard_ch = artists[0]["shard_ch"]

        # Collect the artist ids
        ids = []
        for a in artists:
            if a["shard_ch"] == shard_ch:
                ids.append(a["id"])

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

#    print("on shard '%s' search on: " % shard_ch)
#    for a in artists:
#        print("  %-30s %10d %.3f" % (a["text"][:30], a["id"], a["confidence"]))

    conf_index = { a["id"]:a["confidence"] for a in artists }
    # Make the search request
    req = {
        "artist_ids": ids,
        "artist_name": artist,
        "release_name": release,
        "recording_name": recording,
        "id": str(uuid4())
    }
    try:
        shard = ms.shards[shard_ch]
    except KeyError:
        raise BadRequest("Shard not availble for char '%s'" % shard_ch)

    shards[shard]["in_q"].put(req)
    timeout = monotonic() + SEARCH_TIMEOUT
    count = mismatched = 0
    t0 = monotonic()
    while monotonic() < timeout:
        count += 1
        response = None
        try:
            response = shards[shard]["out_q"].get_nowait()
        except Empty:
            sleep(.0001)
            continue

        if response[2] != req["id"]:
            mismatched += 1
            shards[shard]["out_q"].put(response)
            sleep(.0001)
            continue
                
        break 
    
    t1 = monotonic()
    if response is None:
        print("%d loops were made in %.3fs to FAIL (queue size %d, req %s)" % (count, t1-t0, shards[shard]["out_q"].qsize(), req["id"]))
        raise ServiceUnavailable("Search timed out.")

    hits, duration, _ = response
#    print("%d loops were made in %.3fs to SUCCEED. req took %s, %d mismatches" % (count, t1-t0, duration, mismatched))
    if hits is None or len(hits) < 1:
        raise NotFound("Not found")

    release_id, recording_id, r_conf, _ = hits[0]
    
    results = []
    data = Mapping.select().where((Mapping.release_id == release_id) & (Mapping.recording_id == recording_id))
    for row in data:
        d = model_to_dict(row)
        del d["score"]
        del d["shard_ch"]
        d["r_conf"] = r_conf
        d["time"] = duration
        # TODO: Investigate this exception
        try:
            d["a_conf"] = conf_index[d["artist_credit_id"]]
        except KeyError:
            d["a_conf"] = -1
        del d["artist_credit_id"]
        del d["recording_id"]
        del d["release_id"]
        results.append(d)
        
    return results

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