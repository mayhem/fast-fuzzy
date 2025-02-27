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

    def determine_shards(self):
        """ determine how to break up shards """

        most_letters = "bcdefghijklmnopqrstuvwxyz"
        shard_initials = [ "0123456789a" ]
        shard_initials.extend(self.chunks(most_letters, self.num_shards))

        p_file = os.path.join(self.index_dir, "shard_offsets.pickle")
        with open(p_file, "rb") as f:
            partition_offsets = load(f)

        total_size = partition_offsets[-1][1]
        equal_chunk_size = total_size // self.num_shards

        # Calculate lengths
        for i in range(len(partition_offsets)-1):
            partition_offsets[i].append((partition_offsets[i+1][1] - 1) - partition_offsets[i][1])
        del partition_offsets[-1]


        while True:
            done = True
            for i, part in enumerate(partition_offsets[:-1]):
                other = partition_offsets[i+1]
                if part[2] < equal_chunk_size:
                    part[0] += other[0]
                    part[2] += other[2]
                    del partition_offsets[i+1]
                    done = False
                    break

            if done:
                break

        return [ { "partition_initials": p[0], "offset": p[1], "length": p[2] } for p in partition_offsets ]


    def load_shard(self, shard, offset, length):
        """ load/init the data needed to operate the shard, loads relrecs_offsets for this shard! """

        r_file = os.path.join(self.index_dir, "relrec_offset_table.binary")
        with open(r_file, "rb") as f:
            f.seek(offset)
            data = f.read(length)

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

        self.load_artist_index()
        self.shard = shard


    def load_artist_index(self):
        self.artist_index = FuzzyIndex()
        self.artist_index.load(self.index_dir)

    def load_relrecs_offsets(self):
        t0 = monotonic()
        self.relrec_offsets = []
        r_file = os.path.join(self.index_dir, "relrec_offset_table.binary")
        with open(r_file, "rb") as f:
            while True:
                row = f.read(12)
                if not row:
                    break

                offset, length, id = struct.unpack("III", row)
                self.relrec_offsets.append({ "offset": offset, 
                                             "length": length,
                                             "id": id })
                print(self.relrec_offsets[-1])
        t1 = monotonic()
        print("loaded data in %.1f seconds." % (t1 - t0))

    def load_relrecs_for_artist(self, artist_credit_id):
        """ Load one artist's release and recordings data from disk. Correct relrec_offsets chunk must be loaded. """

        if self._relrec_ids is None:
            self._relrec_ids = [ x["id"] for x in self.relrec_offsets ]
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

    def search(self, artist_name, release_name, recording_name):

        artist_name = FuzzyIndex.encode_string(artist_name)
        recording_name = FuzzyIndex.encode_string(recording_name)
        release_name = FuzzyIndex.encode_string(release_name)

        artists = self.artist_index.search(artist_name)
        results = []

        # TODO: Add release searching, detuning, etc
        # For each hit, search recordings.
        for artist in artists:
            if artist["confidence"] > ARTIST_CONFIDENCE_THRESHOLD:

                # Fetch the index for the recordings -- if not built yet, build it!
                if artist["id"] not in self.relrec_recording_indexes:
                    load_relrecs_for_artist(artist["id"])

                rec_results = self.relrec_recording_indexes[artist["id"]].search(recording_name)
                for result in rec_results:
                    results.append({ "artist_name": artist["text"],
                                     "artist_mbids": artist["artist_mbids"],
                                     "artist_confidence": artist["confidence"],
                                     "recording_name": result["recording_name"],
                                     "recording_mbid": result["recording_mbid"],
                                     "recording_confidence": result["confidence"] })

        return results


if __name__ == "__main__":
    s = MappingLookupSearch("index", 8)
    s.determine_shards()
#    s.load_shard(0, 0, 16416303)
#    s.load_relrecs_for_artist(65)
