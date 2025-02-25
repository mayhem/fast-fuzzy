#!/usr/bin/env python3

from bisect import bisect_left
from pickle import load
from time import monotonic
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
        self.shards = None

        self.relrec_offsets = None
        self._relrec_ids = None

        self.artist_index = None
        self.relrec_release_indexes = {}
        self.relrec_recording_indexes = {}

    def determine_shards(self):
        """ load the offsets table to determine how to shard this index, loads all relrec_offsets! """

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

        total = len(self.relrec_offsets)
        recs_per_shard = total // self.num_shards

        self.shards = []
        for shard in range(self.num_shards):
            relrec = self.relrec_offsets[shard * recs_per_shard]
            next_relrec = self.relrec_offsets[(shard + 1) * recs_per_shard]
            if shard < self.num_shards - 1:
                length = next_relrec["offset"] - relrec["offset"]
            else:
                eof = self.relrec_offsets[-1]["offset"] + self.relrec_offsets[-1]["length"]
                length = eof - relrec["offset"]
            print("shard %d off: %12d id: %12d len: %12d" % (shard, relrec["offset"], relrec["id"], length))
            self.shards.append({ "offset": relrec["offset"], "id": relrec["id"], "length": relrec["length"] })


    def load_shard(self, shard, offset, length):
        """ load/init the data needed to operate the shard, loads relrecs_offsets for this shard! """

#        offset = self.shards[shard]["offset"]
#        length = self.shards[shard]["length"]

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


    def load_artist_index(self):
        self.artist_index = FuzzyIndex()
        self.artist_index.load(self.index_dir)

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

        t0 = monotonic()
        artists = self.artist_index.search(artist_name)
        t1 = monotonic()
        results = []

        # For each hit, search recordings.
        for artist in artists:
            if artist["confidence"] > ARTIST_CONFIDENCE_THRESHOLD:

                # Fetch the index for the recordings -- if not built yet, build it!
                if 
                    index = FuzzyIndex()
                    index.build(artist["recording_data"], "text")
                    artist["index"] = index

                rec_results = artist["index"].search(recording_name)
                for result in rec_results:
                    results.append({ "artist_name": artist["text"],
                                     "artist_mbids": artist["artist_mbids"],
                                     "artist_confidence": artist["confidence"],
                                     "recording_name": result["recording_name"],
                                     "recording_mbid": result["recording_mbid"],
                                     "recording_confidence": result["confidence"] })
#            else:
#                print("Artist '%s' %.1f ignored" % (artist["text"], artist["confidence"]))



if __name__ == "__main__":
    s = MappingLookupSearch("small_index", 8)
#    s.determine_shards()
    s.load_shard(0, 0, 16416303)
    s.load_relrecs_for_artist(65)
