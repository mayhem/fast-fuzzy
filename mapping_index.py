#!/usr/bin/env python3

from time import monotonic
from pickle import dumps, dump
import os
from struct import pack
import sys

import psycopg2
from psycopg2.extras import DictCursor, execute_values

from utils import IndexDataPickleList
from fuzzy_index import FuzzyIndex

# TODO: Remove _ from the combined field of canonical data dump. Done, but make PR

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

        last_artist_combined = None
        last_row = None
        current_part_id = None

        t0 = monotonic()
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
                                  , combined_lookup
                               FROM mapping.canonical_musicbrainz_data
                               JOIN recording rec
                                 ON rec.gid = recording_mbid
                               JOIN release rel
                                 ON rel.gid = release_mbid
                              WHERE artist_credit_id < 10000
                           ORDER BY artist_credit_id""")

            print("load data")
            for i, row in enumerate(curs):
                if i == 0:
                    continue

                if i % 1000000 == 0:
                    print("Indexed %d rows" % i)

                if last_row is not None and row["artist_credit_id"] != last_row["artist_credit_id"]:
                    # Save artist data for artist index
        
                    # Remove duplicate release/id entries
                    release_data = [dict(t) for t in {tuple(d.items()) for d in release_data}]

                    # Save all the artist, release and recording data into ram. 
                    # What could possibly go wrong?
                    encoded = FuzzyIndex.encode_string(last_row["artist_credit_name"])
                    artist_data.append({ "text": encoded,
                                         "index": last_row["artist_credit_id"],
                                         "release_data": release_data,
                                         "recording_data": recording_data })
                    recording_data = []
                    release_data = []
       
                    # Write out the release/recording data and update artist_offsets

                recording_data.append({ "text": FuzzyIndex.encode_string(row["recording_name"]) or "",
                                        "id": row["recording_id"] })
                release_data.append({ "text": FuzzyIndex.encode_string(row["release_name"]) or "",
                                      "id": row["release_id"] })

                last_row = row

            # dump out the last bits of data
            encoded = FuzzyIndex.encode_string(row["artist_credit_name"])
            artist_data.append({ "text": encoded,
                                 "index": row["artist_credit_id"],
                                 "release_data": release_data,
                                 "recording_data": recording_data })
            recording_data = []
            release_data = []


        # (encoded_artist_name, artist)
        artist_sort_data = []
        for artist in artist_data:
            artist_sort_data.append([artist["text"], artist])

        # Re-sort the data into unidecode space
        sorted_artist_data = sorted(artist_sort_data, key=lambda x: (x[0], x[1]["index"]))
        # Cleanup large chunk of data now
        del artist_sort_data

        shard_offsets = {}
        index_file = os.path.join(index_dir, "relrec_data.pickle")
        with open(index_file, "wb") as relrec_f:
            for artist in sorted_artist_data:
                p_release_data = dumps(artist[1]["release_data"])
                p_recording_data = dumps(artist[1]["recording_data"])
                del artist[1]["release_data"]
                del artist[1]["recording_data"]

                relrec_data_size = len(p_release_data) + len(p_recording_data)
                relrec_offsets.append({ "id": artist[1]["index"],
                                        "offset": relrec_offset,
                                        "length": relrec_data_size,
                                        "part_ch": artist[0][0]})

                if artist[0][0] not in shard_offsets:
                    shard_offsets[artist[0][0]] = relrec_offset

                # pickle release/recording data
                relrec_offset += relrec_data_size
                relrec_f.write(p_release_data)
                relrec_f.write(p_recording_data)

            relrec_offset = relrec_f.tell()

        # Finish off the shard_offsets
        print("Write relrec offsets table")
        r_file = os.path.join(index_dir, "relrec_offset_table.binary")
        with open(r_file, "wb") as f:
            for relrec in relrec_offsets:
                f.write(pack("III", relrec["offset"], relrec["length"], relrec["id"]))

        print("Write shard offsets table")
        shard_table = []
        for shard_off in sorted(shard_offsets):
            shard_table.append({ "shard_ch": shard_off, "offset": shard_offsets[shard_off], "length": None })
        for i, entry in enumerate(shard_table):
            try:
                entry["length"] = shard_table[i+1]["offset"] - shard_table[i]["offset"]
            except IndexError:
                entry["length"] = relrec_offset - shard_table[i]["offset"]

        s_file = os.path.join(index_dir, "shard_table.pickle")
        with open(s_file, "wb") as f:
            dump(shard_table, f)

        print("Build artist index")
        self.artist_index.build(artist_data, "text")
        print("Save artist index")
        self.artist_index.save(index_dir)

        t1 = monotonic()
        print("loaded data and build artist index in %.1f seconds." % (t1 - t0))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: mapping_index.py <index dir>")
        sys.exit(-1)

    index_dir = sys.argv[1]

    mi = MappingLookupIndex()
    with psycopg2.connect(DB_CONNECT) as conn:
        try:
            os.makedirs(index_dir)
        except OSError:
            pass

        mi.create(conn, index_dir)
