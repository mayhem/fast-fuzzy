#!/usr/bin/env python3

from multiprocessing import Queue, Process

from search_index import MappingLookupSearch


def mapping_lookup_process(in_q, out_q, index_dir, num_shards, shard):
    ms = MappingLookupSearch(index_dir, num_shards)
    ms.load_shard(shard)

    while True:
        req = in_q.get()

        # Check to see if we should exit
        if "exit" in req:
            return

        out_q.put(ms.search(req))
