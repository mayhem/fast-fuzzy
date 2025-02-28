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

        print("load!")
        for i, shard in enumerate(partition_table):
            print("%d %12s %12s %s" % (i, f'{shard["offset"]:,}', f'{shard["length"]:,}', shard["shard_ch"]))
        print()

        equal_chunk_size = (partition_table[-1]["offset"] + partition_table[-1]["length"]) // self.num_shards

        # This is a very pathetic attempt at sharding. This needs real life input to be improved
        while True:
            done = True
            for i, part in enumerate(partition_table[:-1]):
                next_row = partition_table[i+1]
                if part["length"] < equal_chunk_size:
                    part["shard_ch"] += next_row["shard_ch"]
                    part["length"] += next_row["length"]
                    del partition_table[i+1]
                    done = False
                    break

            for i, shard in enumerate(partition_table):
                print("%d %12s %12s %s" % (i, f'{shard["offset"]:,}', f'{shard["length"]:,}', shard["shard_ch"]))
            print()

            if done or len(partition_table) == self.num_shards:
                break

        self.shards = partition_table
        print("done!")


    def load_shard(self, shard):
        """ load/init the data needed to operate the shard, loads relrecs_offsets for this shard! """

        print("in load shard")
        if self.shards is None:
            self.split_shards()

        offset = self.shards[shard]["offset"]
        length = self.shards[shard]["length"]
        self.shard = shard

        r_file = os.path.join(self.index_dir, "relrec_offset_table.binary")
        with open(r_file, "rb") as f:
            f.seek(offset)
            data = f.read(length)

        print("read %d bytes from %d" % (len(data), offset))

        d_offset = 0
        self.relrec_offsets = []
        while True:
            try:
                offset, length, id = struct.unpack("III", data[d_offset:d_offset+12])
            except struct.error:
                break

            self.relrec_offsets.append({ "offset": offset, 
                                         "length": length,
                                         "id": id })
            d_offset += 12

        print("%d rows in offsets" % len(self.relrec_offsets))


    def load_relrecs_for_artist(self, artist_credit_id):
        """ Load one artist's release and recordings data from disk. Correct relrec_offsets chunk must be loaded. """

        # Have we loaded this already? If so, bail!
        if artist_credit_id in self.relrec_release_indexes:
            print("aready have artist")
            return

        if self._relrec_ids is None:
            self._relrec_ids = [ x["id"] for x in self.relrec_offsets ]
#        print(self._relrec_ids)
        offset = bsearch(self._relrec_ids, artist_credit_id)
        if offset < 0:
            print("artist not found")
            return
        relrec = self.relrec_offsets[offset]
        print("found artist")

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

        print(req)

        artist_ids = req["artist_ids"]
        artist_name = FuzzyIndex.encode_string(req["artist_name"])
        recording_name = FuzzyIndex.encode_string(req["recording_name"])
        release_name = FuzzyIndex.encode_string(req["release_name"])

        results = []
        for artist_id in artist_ids:
            self.load_relrecs_for_artist(artist_id)
            rec_index = self.relrec_recording_indexes[artist_id]
            rec_results = rec_index.search(recording_name, min_confidence=RECORDING_CONFIDENCE)
            for result in rec_results:
                results.append({ "artist_name": artist_name, 
                                 "artist_credit_id": artist_id,
                                 "recording_name": recording_name,
                                 "recording_id": result["index"],
                                 "recording_confidence": result["confidence"] })

        return results


if __name__ == "__main__":
    s = MappingLookupSearch("index", 8)
    s.split_shards()
    s.load_shard(0)
    results = s.search([65], "portishead", "dummy", "strangers")
    print(results)
