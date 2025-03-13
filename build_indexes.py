#!/usr/bin/env python3

import concurrent.futures
from time import monotonic
import os
import sys

from peewee import *
from tqdm import tqdm

from fuzzy_index import FuzzyIndex
from database import Mapping, IndexCache, open_db, db
from search_index import MappingLookupSearch
from shared_mem_cache import SharedMemoryArtistDataCache

BATCH_SIZE = 500

bi = None

def build_data(lst):
    bi.build_artist_data_index(lst)
    return True
        
class BuildIndexes:
    
    def __init__(self, index_dir, temp_dir, num_procs=1):
        self.index_dir = index_dir
        self.temp_dir = temp_dir
        self.num_procs = num_procs
        self.cache = SharedMemoryArtistDataCache(temp_dir, 0)
        self.ms = MappingLookupSearch(self.cache, index_dir)

    def build_artist_data_index(self, artist_list):
        batch = []
        for artist_credit_id in artist_list:
            data = self.ms.load_artist(artist_credit_id, dont_cache=True)
            try:
                pickled = self.cache.pickle_data(data)
            except TypeError:
                pickled = None
            
            batch.append((artist_credit_id, pickled))
            if len(batch) >= BATCH_SIZE:
                with db.atomic() as transaction:
                    for ac_id, pickled in batch:
                        while True:
                            try:
                                index = IndexCache().create(artist_credit_id=ac_id, artist_data=pickled)
                                index.replace()
                                break
                            except OperationalError:
                                sleep(.1)
                            
                batch = []

        if batch:
            with db.atomic() as transaction:
                for ac_id, pickled in batch:
                    while True:
                        try:
                            index = IndexCache().create(artist_credit_id=ac_id, artist_data=pickled)
                            index.replace()
                            break
                        except OperationalError:
                            sleep(.1)
            self.batch = []
            
    def build_data(self, lst):
        for l in lst:
            build_artist_data_index(l)
            
    def build(self):
        cur = db.execute_sql("""select count(*) as cnt
                                 from mapping
                            left join index_cache
                                   on mapping.artist_credit_id = index_cache.artist_credit_id
                                where index_cache.artist_credit_id is null""")
        rows = cur.fetchone()[0]
        cur = db.execute_sql("""select mapping.artist_credit_id, count(*) as cnt
                                 from mapping
                            left join index_cache
                                   on mapping.artist_credit_id = index_cache.artist_credit_id
                                where index_cache.artist_credit_id is null
                             group by mapping.artist_credit_id order by cnt desc""")
        
        proc_data = []
        cur_chunk = []
        for row in enumerate(cur.fetchall()):
            cur_chunk.append(row[0])
            if len(cur_chunk) >= BATCH_SIZE:
                proc_data.append(cur_chunk)
                cur_chunk = []
                
        print("got %s chunks!" % len(proc_data))
        with tqdm(total=len(proc_data)) as t:
            with concurrent.futures.ProcessPoolExecutor(max_workers=self.num_procs) as exe:
                future_to_batch = {exe.submit(build_data, data): i for i, data in enumerate(proc_data) }
                for future in concurrent.futures.as_completed(future_to_batch):
                    t.update(1)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: build_indexes.py <index dir> <num procs>")
        sys.exit(-1)

    index_dir = sys.argv[1]
    num_procs = sys.argv[2]
    open_db(os.path.join(index_dir, "mapping.db"))
    bi = BuildIndexes(index_dir, "/mnt/tmpfs", int(num_procs))
    bi.build()
