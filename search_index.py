#!/usr/bin/env python3

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
        self.relrec_combined_indexes = {}

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
        if artist_credit_id in self.relrec_combined_indexes:
            return True

        recording_data = []
        combined_data = []
        for row in Mapping.select().where(Mapping.artist_credit_id == artist_credit_id):
            recording_data.append({ "id": row.recording_id,
                                    "text": FuzzyIndex.encode_string(row.recording_name),
                                    "release": row.release_id,
                                    "score": row.score })
            combined_data.append({ "id": row.recording_id,
                                   "text": FuzzyIndex.encode_string(row.release_name) +
                                           FuzzyIndex.encode_string(row.recording_name),
                                   "release": row.release_id,
                                   "score": row.score })
#            print("'%s' '%s' '%s'" % (row.release_name, row.recording_name, 
#                                      FuzzyIndex.encode_string(row.release_name) +
#                                      FuzzyIndex.encode_string(row.recording_name)))
            
        recording_index = FuzzyIndex()
        if recording_data:
            recording_index.build(recording_data, "text")
        else:
            return False

        combined_index = FuzzyIndex()
        if combined_data:
            combined_index.build(combined_data, "text")
        else:
            return False

        self.relrec_recording_indexes[artist_credit_id] = (recording_index, recording_data)
        self.relrec_combined_indexes[artist_credit_id] = (combined_index, combined_data)

        return True

    def search(self, req):

        artist_ids = req["artist_ids"]
        artist_name = FuzzyIndex.encode_string(req["artist_name"])
        release_name = FuzzyIndex.encode_string(req["release_name"])
        recording_name = FuzzyIndex.encode_string(req["recording_name"])

        print(f"      ids:", artist_ids)
        print(f"   artist: {artist_name:<30} {req['artist_name']:<30}")
        print(f"  release: {release_name:<30} {req['release_name']:<30}")
        print(f"recording: {recording_name:<30} {req['recording_name']:<30}")
        print()
        
        open_db(self.db_file)

        results = []
        for artist_id in artist_ids:
            print("artist %d ------------------------------------" % artist_id)
            if not self.load_artist(artist_id):
                print(f"artist {artist_id} not found on this shard. {self.shard}")
                continue

            if release_name:
                try:
                    combined_index, combined_data = self.relrec_combined_indexes[artist_id]
                except KeyError:
                    print("relrecs for '%s' not found on this shard." % req["artist_name"])
                    continue

                rel_results = combined_index.search(release_name + recording_name, min_confidence=RELEASE_CONFIDENCE)
                rel_results = sorted(rel_results, key=lambda r: (r["confidence"], r["score"]))
                print("    release results for '%s'" % release_name)
                if rel_results:
                    for res in rel_results:
                        print("        %-8d %-30s %.2f %d" % (res["id"], res["text"], res["confidence"], res["release"]))
                    print()
                else:
                    print("    ** No release results **")
                return [ (r["release"], r["id"]) for r in rel_results[:3] ]

            else:
                try:
                    rec_index, recording_data = self.relrec_recording_indexes[artist_id]
                except KeyError:
                    print("relrecs for '%s' not found on this shard." % req["artist_name"])
                    continue

                rec_results = rec_index.search(recording_name, min_confidence=RECORDING_CONFIDENCE)
                rec_results = sorted(rec_results, key=lambda r: (r["confidence"], r["score"]))
                print("    recording results for '%s'" % recording_name)
                if rec_results:
                    for res in rec_results:
                        print("       %-8d %-30s %.2f %d" % (res["id"], res["text"], res["confidence"], res["release"]))
                else:
                    print("    ** No recording results **")
                print()
                
                return [ (r["release"], r["id"]) for r in rec_results[:3] ]
                

if __name__ == "__main__":
    from tabulate import tabulate
    s = MappingLookupSearch("index", 8)
    s.split_shards()
    results = s.search({ "artist_ids": [65], "artist_name": "portishead", "release_name": "dummy", "recording_name": "strangers" })
    results = s.search({ "artist_ids": [963], "artist_name": "morecheeba", "release_name": "who can you tryst", "recording_name": "trigger hippie" })
    print(tabulate(results))
