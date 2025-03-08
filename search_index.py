#!/usr/bin/env python3

from collections import defaultdict
from math import ceil
from pickle import load
from random import randint
from time import monotonic, sleep
from struct import unpack
import struct
import os
import sys

from tabulate import tabulate

from fuzzy_index import FuzzyIndex
from shard_histogram import shard_histogram
from utils import split_dict_evenly
from database import Mapping
from database import open_db

RELEASE_CONFIDENCE = .5
RECORDING_CONFIDENCE = .5


class MappingLookupSearch:

    def __init__(self, index_dir, num_shards):
        self.index_dir = index_dir
        self.num_shards = num_shards
        self.shard = None
        self.shards = None

        self.relrec_offsets = None
        self._relrec_ids = None

        self.artist_index = None
        self.relrec_recording_indexes = {}
        self.relrec_release_indexes = {}

        self.db_file = os.path.join(index_dir, "mapping.db")

    @staticmethod
    def chunks(l, n):
        d, r = divmod(len(l), n)
        for i in range(n):
            si = (d+1)*(i if i < r else r) + d*(0 if i < r else i - r)
            yield l[si:si+(d+1 if i < r else d)]

    def split_shards(self):
        """ determine how to break up shards """

        # Split the dict based on an even distribution of the value's sums
        split_hist = split_dict_evenly(shard_histogram, self.num_shards)

        self.shards = {}
        for shard, pairs in enumerate(split_hist):
            for ch in pairs:
                self.shards[ch] = shard

    def load_artist(self, artist_credit_id):
        """ Load one artist's release and recordings data from disk. Correct relrec_offsets chunk must be loaded. """

        # Have we loaded this already? If so, bail!
        if artist_credit_id in self.relrec_release_indexes:
            return True

        recording_data = []
        recording_releases = defaultdict(list)
        release_data = []
        release_ref = {}
        data = Mapping.select().where(Mapping.artist_credit_id == artist_credit_id)
        for row in data:
            recording_data.append({ "id": row.recording_id,
                                    "text": FuzzyIndex.encode_string(row.recording_name),
                                    "release": row.release_id,
                                    "score": row.score })
            recording_releases[row.recording_id].append(row.release_id)
            encoded = FuzzyIndex.encode_string(row.release_name)
            if encoded:
                release_data.append({ "id": row.recording_id,
                                       "text": FuzzyIndex.encode_string(row.release_name),
                                       "release": row.release_id,
                                       "score": row.score })
                release_ref[encoded] = row.release_id 
#            print("'%s' '%s' '%s'" % (row.release_name, row.recording_name, 
#                                      FuzzyIndex.encode_string(row.release_name) +
#                                      FuzzyIndex.encode_string(row.recording_name)))
            
        recording_index = FuzzyIndex()
        if recording_data:
            t0 = monotonic()
            l = [ x["text"] for x in recording_data]
            s = set(l)
            print("%d vs %d" % (len(l), len(s)))
            recording_index.build(recording_data, "text")
            print("rec index %.3fms (%d items)" % ((monotonic() - t0) * 1000, len(recording_data)))
        else:
            return False

        release_index = FuzzyIndex()
        if release_data:
            flatt = [ { "text": k, "id": v } for k,v in release_ref.items() ]
            t0 = monotonic()
            release_index.build(flatt, "text")
            print("release index %.3fms (%d items)" % ((monotonic() - t0) * 1000, len(flatt)))
        else:
            return False

        self.relrec_recording_indexes[artist_credit_id] = recording_index
        self.relrec_release_indexes[artist_credit_id] = (release_index, recording_releases)

        return True

    def search(self, req):

        artist_ids = req["artist_ids"]
        artist_name = FuzzyIndex.encode_string(req["artist_name"])
        release_name = FuzzyIndex.encode_string(req["release_name"])
        recording_name = FuzzyIndex.encode_string(req["recording_name"])

#        print(f"      ids:", artist_ids)
#        print(f"   artist: {artist_name:<30} {req['artist_name']:<30}")
#        print(f"  release: {release_name:<30} {req['release_name']:<30}")
#        print(f"recording: {recording_name:<30} {req['recording_name']:<30}")
#        print()
        
        open_db(self.db_file)

        # TODO: Consider the other artists too -- you just pick the best of the first!
        results = []
        for artist_id in artist_ids:
#            print("artist %d ------------------------------------" % artist_id)
            if not self.load_artist(artist_id):
#                print(f"artist {artist_id} not found on this shard. {self.shard}")
                continue

            try:
                rec_index = self.relrec_recording_indexes[artist_id]
            except KeyError:
                print("relrecs for '%s' not found on this shard." % req["artist_name"])
                continue

            rec_results = rec_index.search(recording_name, min_confidence=RECORDING_CONFIDENCE)
            rec_results = sorted(rec_results, key=lambda r: (-r["confidence"], r["score"]))
#            print("    recording results for '%s'" % recording_name)
#            if rec_results:
#                for res in rec_results:
#                    print("       %-8d %-30s %.2f %d %d" % (res["id"], res["text"], res["confidence"], res["release"], res["score"]))
#            else:
#                print("    ** No recording results **")
#            print()

            if not release_name:
                return [ (r["release"], r["id"], r["confidence"]) for r in rec_results[:3] ]

            try:
                release_index, recording_releases = self.relrec_release_indexes[artist_id]
            except KeyError:
                print("relrecs for '%s' not found on this shard." % req["artist_name"])
                continue

            rel_results = release_index.search(release_name, min_confidence=RELEASE_CONFIDENCE)
            rel_results = sorted(rel_results, key=lambda r: -r["confidence"])
#            print("    release results for '%s'" % release_name)
#            if rel_results:
#                for res in rel_results:
#                    print("        %-8d %-30s %.2f" % (res["id"], res["text"], res["confidence"]))
#                print()
#            else:
#                print("    ** No release results **")


            RESULT_THRESHOLD = .5
            hits = []
            for rec_res in rec_results:
                if rec_res["confidence"] < RESULT_THRESHOLD:
                    break
                for rel_res in rel_results:
                    if rel_res["confidence"] < RESULT_THRESHOLD:
                        break
                    if rec_res["id"] in recording_releases:
                        hits.append({ "recording_id": rec_res["id"],
                                      "recording_text": rec_res["text"],
                                      "recording_conf": rec_res["confidence"],
                                      "score": rec_res["score"],
                                      "release_id": rel_res["id"],
                                      "release_name": rel_res["text"],
                                      "release_conf": rel_res["confidence"],
                                      "confidence": (rec_res["confidence"] + rel_res["confidence"])/2 })
                        
            if not hits:
                return []
                        
#            print("    combined results for '%s'" % release_name)
#            hits = sorted(hits, key=lambda h: h["confidence"], reverse=True)
#            if hits:
#                for hit in hits:
#                    print("        %-8d %-30s %.2f %d %d %-30s %.2f -> %.2f" % (hit["recording_id"],
#                                                             hit["recording_text"],
#                                                             hit["recording_conf"],
#                                                             hit["score"],
#                                                             hit["release_id"],
#                                                             hit["release_name"],
#                                                             hit["release_conf"],
#                                                             hit["confidence"]
#                                                              ))
#                 print()
#             else:
#                 print("    ** No hits **")

            return [ (hits[0]["release_id"], hits[0]["recording_id"], hits[0]["confidence"]) ]
                

if __name__ == "__main__":
    from tabulate import tabulate
    s = MappingLookupSearch("index", 8)
    s.split_shards()
    results = s.search({ "artist_ids": [65], "artist_name": "portishead", "release_name": "dummy", "recording_name": "strangers" })
    results = s.search({ "artist_ids": [963], "artist_name": "morecheeba", "release_name": "who can you tryst", "recording_name": "trigger hippie" })
    print(tabulate(results))
