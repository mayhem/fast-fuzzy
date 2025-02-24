#!/usr/bin/env python3

from time import monotonic
from pickle import dumps, dump
import os
import sys

import psycopg2
from psycopg2.extras import DictCursor, execute_values

from utils import IndexDataPickleList
from fuzzy_index import FuzzyIndex

# For wolf
DB_CONNECT = "dbname=musicbrainz_db user=musicbrainz host=localhost port=5432 password=musicbrainz"

# For wolf/docker
#DB_CONNECT = "dbname=musicbrainz_db user=musicbrainz host=musicbrainz-docker_db_1 port=5432 password=musicbrainz"

ARTIST_CONFIDENCE_THRESHOLD = .45
CHUNK_SIZE = 100000
MAX_THREADS = 8


class MappingLookupIndex:

    def create(self, conn, index_dir):

        # TODO: VA and more complex artist credits probably not handled correctly
        self.artist_index = FuzzyIndex()

        last_artist_credit_id = -1
        last_row = None
        current_part_ch = None
        partitions = []

        t0 = monotonic()

        index_file = os.path.join(index_dir, "relrec_data.pickle")
        with open(index_file, "wb") as relrec_f:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                artist_data = []
                recording_data = []
                release_data = []
                relrec_offsets = []
                relrec_offset = 0

                print("execute query")
                curs.execute(""" SELECT artist_credit_id
                                      , artist_mbids
                                      , artist_credit_name
                                      , release_name
                                      , rel.id AS release_id
                                      , recording_name
                                      , rec.id AS recording_id
                                   FROM mapping.canonical_musicbrainz_data
                                   JOIN recording rec
                                     ON rec.gid = recording_mbid
                                   JOIN release rel
                                     ON rel.gid = release_mbid
                                  WHERE artist_credit_id < 1000
                               ORDER BY artist_credit_id""")

                print("load data")
                for i, row in enumerate(curs):
                    if i == 0:
                        continue

                    if i % 1000000 == 0:
                        print("Indexed %d rows" % i)

                    if last_artist_credit_id >= 0 and row["artist_credit_id"] != last_artist_credit_id:
                        # Save artist data for artist index
                        encoded = FuzzyIndex.encode_string(last_row["artist_credit_name"])
                        artist_data.append({ "text": encoded, "index": None })
                        if current_part_ch != encoded[0]:
                            partitions.append({ "ch": encoded[0], "offset": relrec_offset })
                            current_part_ch = encoded[0]
            
                        # Remove duplicate release/id entries
                        release_data = [dict(t) for t in {tuple(d.items()) for d in release_data}]

                        # pickle release/recording data
                        p_release_data = dumps(release_data)
                        p_recording_data = dumps(recording_data)
                        recording_data = []
                        release_data = []
           
                        # Write out the release/recording data and update artist_offsets
                        relrec_data_size = len(p_release_data) + len(p_recording_data)
                        relrec_offsets.append({ "id": last_row["artist_credit_name"],
                                                "offset": relrec_offset,
                                                "length": relrec_data_size })
                        relrec_offset += relrec_data_size
                        relrec_f.write(p_release_data)
                        relrec_f.write(p_recording_data)

                    recording_data.append({ "text": FuzzyIndex.encode_string(row["recording_name"]) or "",
                                            "id": row["recording_id"] })
                    release_data.append({ "text": FuzzyIndex.encode_string(row["release_name"]) or "",
                                          "id": row["release_id"] })

                    last_row = row
                    last_artist_credit_id = row["artist_credit_id"]

                # dump out the last bits of data
                p_release_data = dumps(release_data)
                p_recording_data = dumps(recording_data)
                relrec_data_size = len(p_release_data) + len(p_recording_data)
                relrec_offsets.append({ "id": last_row["artist_credit_name"],
                                        "offset": relrec_offset,
                                        "length": relrec_data_size })
                relrec_f.write(p_release_data)
                relrec_f.write(p_recording_data)

        print("Write relrec offsets table")
        r_file = os.path.join(index_dir, "relrec_offset_table.pickle")
        with open(r_file, "wb") as f:
            dump(relrec_offsets, f)
        print("Write partitions table")
        p_file = os.path.join(index_dir, "partitions_offset_table.pickle")
        with open(p_file, "wb") as f:
            dump(partitions, f)
        print("Build artist index")
        self.artist_index.build(artist_data, "text")
        print("Save artist index")
        self.artist_index.save(index_dir)

        t1 = monotonic()
        print("loaded data and build artist index in %.1f seconds." % (t1 - t0))


if __name__ == "__main__":
    mi = MappingLookupIndex()
    with psycopg2.connect(DB_CONNECT) as conn:
        mi.create(conn, "index")
