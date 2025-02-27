#!/usr/bin/env python3

from multiprocessing import Queue, Process

from mapping_search import MappingLookupSearch


class MappingLookupProcess:

    def __init__(self, in_q, out_q, index_dir, shard):
        self.in_q = in_q
        self.out_q = out_q
        self.index_dir = index_dir
        self.shard = shard

        self.ms = MappingLookupSearch(index_dir)
        shard_info = self.ms.split_shards()
        self.ms.load_shard(shard, shard_info["offset"], shard_info["length"])

    def run(self):

        while True:
            req = self.in_q.get()


