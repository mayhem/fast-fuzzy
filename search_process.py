#!/usr/bin/env python3

from multiprocessing import Queue, Process
from time import monotonic

from search_index import MappingLookupSearch


def mapping_lookup_process(in_q, out_q, index_dir, num_shards, shard):
    ms = MappingLookupSearch(index_dir, num_shards)
    while True:
        req = in_q.get()

        # Check to see if we should exit
        if "exit" in req:
            return

        t0 = monotonic()
        ret = ms.search(req)
        print("search proc: ", ret)
        out_q.put((ret, "%.3fms" % ((monotonic() - t0) * 1000)))
