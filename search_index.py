#!/usr/bin/env python3

from bisect import bisect_left
from time import monotonic
from pickle import loads, load
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
        self._ids = None

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
                self.relrec_offsets.append({ "offset": int.from_bytes(row[0:3], byteorder='little'),
                                             "length": int.from_bytes(row[4:7], byteorder='little'),
                                             "id": int.from_bytes(row[8:11], byteorder='little')})
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


    def load_shard(self, shard):
        """ load/init the data needed to operate the shard, loads relrecs_offsets for this shard! """

        offset = self.shards[shard]["offset"]
        length = self.shards[shard]["length"]

        r_file = os.path.join(self.index_dir, "relrec_offset_table.pickle")
        with open(r_file, "rb") as f:
            f.seek(offset)
            data = f.read(length)

            self.relrec_offsets = load(f)


    def load_relrecs_for_artist(self, artist_credit_id):
        """ Load one artist's release and recordings data from disk. Correct relrec_offsets chunk must be loaded. """

        if self._ids is None:
            self._ids = [ x["id"] for x in self.relrec_offsets ]
        offset = bsearch(self._ids, artist_credit_id)
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

        print(release_data)

if __name__ == "__main__":
    s = MappingLookupSearch("small_index", 8)
    s.determine_shards()
    s.load_relrecs_for_artist(65)
