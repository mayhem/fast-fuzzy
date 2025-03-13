#!/usr/bin/env python3

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

class BuildIndexes:
    
    def __init__(self, index_dir, temp_dir):
        self.index_dir = index_dir
        self.temp_dir = temp_dir
        self.cache = SharedMemoryArtistDataCache(temp_dir, 0)
        self.ms = MappingLookupSearch(self.cache, index_dir)
        self.batch = []

    
    def build_artist_data_index(self, artist_credit_id):
        data = self.ms.load_artist(artist_credit_id, dont_cache=True)
        try:
            pickled = self.cache.pickle_data(data)
        except TypeError:
            return
        
        self.batch.append((artist_credit_id, pickled))
        if len(self.batch) >= BATCH_SIZE:
            with db.atomic() as transaction:
                for ac_id, pickled in self.batch:
                    index = IndexCache().create(artist_credit_id=ac_id, artist_data=pickled)
                    index.replace()
            self.batch = []
        
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
        with tqdm(total=rows) as t:
            for row in cur.fetchall():
                t.write("artist %d rows: %d" % (row[0], row[1]))
                self.build_artist_data_index(row[0])
                t.update(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: build_indexes.py <index dir>")
        sys.exit(-1)

    index_dir = sys.argv[1]
    open_db(os.path.join(index_dir, "mapping.db"))
    bi = BuildIndexes(index_dir, "/mnt/tmpfs")
    bi.build()
