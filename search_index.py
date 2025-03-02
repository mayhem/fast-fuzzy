#!/usr/bin/env python3

from bisect import bisect_left
from math import ceil
from pickle import load
from random import randint
from time import monotonic, sleep
from struct import unpack
import struct
import os
import sys

from fuzzy_index import FuzzyIndex
from shard_histogram import shard_histogram
from utils import split_dict_evenly

RELEASE_CONFIDENCE = .5
RECORDING_CONFIDENCE = .5

def bsearch(alist, item):
    i = bisect_left(alist, item)
    if i != len(alist) and alist[i] == item:
        return i
    return -1


class MappingLookupSearch:

    def __init__(self, index_dir, num_shards):
        self.index_dir = index_dir
        self.num_shards = num_shards
        self.shard = None
        self.shards = None

        self.relrec_offsets = None
        self._relrec_ids = None

        self.artist_index = None
        self.relrec_release_indexes = {}
        self.relrec_recording_indexes = {}

    @staticmethod
    def chunks(l, n):
        d, r = divmod(len(l), n)
        for i in range(n):
            si = (d+1)*(i if i < r else r) + d*(0 if i < r else i - r)
            yield l[si:si+(d+1 if i < r else d)]

    def split_shards(self):
        """ determine how to break up shards """

        p_file = os.path.join(self.index_dir, "shard_table.pickle")
        with open(p_file, "rb") as f:
            partition_table = load(f)

        print("Loaded partition table")
        for i, shard in enumerate(partition_table):
            print("%d %12s %12s %s" % (i, f'{shard["offset"]:,}', f'{shard["length"]:,}', shard["shard_ch"]))
        print()

        self.shards = partition_table
        return

        # Split the dict based on an even distribution of the value's sums
        split_hist = split_dict_evenly(shard_histogram, self.num_shards)

        # Lookup which partition each element should be in
        shards = [ [] for i in range(self.num_shards) ]
        for partition in partition_table:
            for shard, shard_chars in enumerate(split_hist):
                if partition["shard_ch"] in shard_chars:
                    shards[shard].append((partition["offset"], partition["length"], partition["shard_ch"]))

        # sort to combine all the rows in one shard into a single row
        self.shards = []
        for shard in shards:
            length = 0
            chars = ""
            offset = None
            for row in shard:
                if offset is None:
                    offset = row[0]
                length += row[1]
                chars += row[2]
            self.shards.append({ "offset": offset, "length": length, "shard_ch": chars })

#        print(f"partition table")
#        for i, shard in enumerate(self.shards):
#            print("%d %12s %12s %s" % (i, f'{shard["offset"]:,}', f'{shard["length"]:,}', shard["shard_ch"]))
#        print()


    def load_shard(self):
        """ load/init the data needed to operate loads relrecs_offsets! """

        if self.shards is None:
            self.split_shards()

        r_file = os.path.join(self.index_dir, "relrec_offset_table.pickle")
        with open(r_file, "rb") as f:
            relrec_offsets = load(f)
        self.relrec_offsets = sorted(relrec_offsets, key=lambda x: x["id"])


    def load_relrecs_for_artist(self, artist_credit_id):
        """ Load one artist's release and recordings data from disk. Correct relrec_offsets chunk must be loaded. """

        # Have we loaded this already? If so, bail!
        if artist_credit_id in self.relrec_release_indexes:
            print("aready have artist")
            return

        if self._relrec_ids is None:
            self._relrec_ids = [ x["id"] for x in self.relrec_offsets ]
        offset = bsearch(self._relrec_ids, artist_credit_id)
        if offset < 0:
            print("artist not found")
            return
        relrec = self.relrec_offsets[offset]

        r_file = os.path.join(self.index_dir, "relrec_data.pickle")
        with open(r_file, "rb") as f:
            f.seek(relrec["offset"])
            release_data = load(f)
            recording_data = load(f)

        release_index = FuzzyIndex()
        release_index.build(release_data, "text")
        recording_index = FuzzyIndex()
        recording_index.build(recording_data, "text")

        self.relrec_release_indexes[artist_credit_id] = release_index
        self.relrec_recording_indexes[artist_credit_id] = recording_index

    def search(self, req):

        artist_ids = req["artist_ids"]
        artist_name = FuzzyIndex.encode_string(req["artist_name"])
        recording_name = FuzzyIndex.encode_string(req["recording_name"])
        release_name = FuzzyIndex.encode_string(req["release_name"])

        results = []
        for artist_id in artist_ids:
            try:
                self.load_relrecs_for_artist(artist_id)
            except KeyError:
                print("artist '%s' not found on this shard." % req["artist_name"])
                continue

            try:
                rec_index = self.relrec_recording_indexes[artist_id]
            except KeyError:
                print("relrecs for '%s' not found on this shard." % req["artist_name"])
                continue

            rec_results = rec_index.search(recording_name, min_confidence=RECORDING_CONFIDENCE)
            for result in rec_results:
                results.append({ "artist_name": artist_name, 
                                 "artist_credit_id": artist_id,
                                 "recording_name": recording_name,
                                 "recording_id": result["id"],
                                 "recording_confidence": result["confidence"] })

        return results


if __name__ == "__main__":
    from tabulate import tabulate
    s = MappingLookupSearch("small_index", 2)
    s.split_shards()
    s.load_shard()
    results = s.search({ "artist_ids": [65], "artist_name": "portishead", "release_name": "dummy", "recording_name": "strangers" })
    results = s.search({ "artist_ids": [963], "artist_name": "morecheeba", "release_name": "who can you tryst", "recording_name": "trigger hippie" })
    print(tabulate(results))
