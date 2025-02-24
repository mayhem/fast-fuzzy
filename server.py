from flask import Flask, request, jsonify
from werkzeug.exceptions import BadRequest

from fuzzy_index import MappingLookupIndex

app = Flask(__name__)

print("Load index")
mi = MappingLookupIndex()
mi.load("index")
print("starting server")

@app.route("/search")
def index():
    global mi
    artist = request.args.get("a", "")
    recording = request.args.get("r", "")
    if not artist or not recording:
        raise BadRequest("a and r must be given")

    return jsonify(mi.search(artist, recording))
