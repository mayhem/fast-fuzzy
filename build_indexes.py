#!/usr/bin/env python3

from time import monotonic
import os
import sys

from peewee import *

from fuzzy_index import FuzzyIndex
from database import Mapping, IndexCache, open_db, db
from search_index import MappingLookupSearch
from shared_mem_cache import SharedMemoryArtistDataCache


class BuildIndexes:
    
    def __init__(self, index_dir, temp_dir):
        self.index_dir = index_dir
        self.temp_dir = temp_dir
        self.cache = SharedMemoryArtistDataCache(temp_dir)
        self.ms = MappingLookupSearch(self.cache, index_dir)

    
    def build_artist_data_index(self, artist_credit_id):
        data = self.ms.load_artist(artist_credit_id, dont_cache=True)
        pickled = self.cache.pickle_data(data)

        index = IndexCache().create(artist_credit_id=artist_credit_id, artist_data=pickled)
        index.replace()
        
    def build(self):
#        query = (Mapping.select(Mapping.artist_credit_id, fn.Count(Mapping.artist_credit_id).alias("cnt"))
#                        .join(IndexCache, IndexCache.artist_credit_id == Mapping.artist_credit_id, JOIN.LEFT_OUTER)
#                        .where(IndexCache.artist_credit_id.is_null())
#                        .group_by(Mapping.artist_credit_id)
#                        .order_by(fn.Count(Mapping.artist_credit_id).desc()))
        cur = db.execute_sql("""select mapping.artist_credit_id, count(*) as cnt
                                 from mapping
                            left join index_cache
                                   on mapping.artist_credit_id = index_cache.artist_credit_id
                                where index_cache.artist_credit_id is null
                             group by mapping.artist_credit_id order by cnt desc""")
        for row in cur.fetchall():
            print("artist %d rows: %d" % (row[0], row[1]))
            self.build_artist_data_index(row[0])


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: build_indexes.py <index dir>")
        sys.exit(-1)

    index_dir = sys.argv[1]
    open_db(os.path.join(index_dir, "mapping.db"))
    bi = BuildIndexes(index_dir, "/mnt/tmpfs")
    bi.build()
