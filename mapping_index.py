#!/usr/bin/env python3

from time import monotonic
from pickle import dumps, dump
import os
from struct import pack
import sys

from alphabet_detector import AlphabetDetector
import psycopg2
from psycopg2.extras import DictCursor, execute_values

from fuzzy_index import FuzzyIndex
from database import Mapping, create_db

# TODO: Remove _ from the combined field of canonical data dump. Done, but make PR

# For wolf
DB_CONNECT = "dbname=musicbrainz_db user=musicbrainz host=localhost port=5432 password=musicbrainz"

# For wolf/docker
#DB_CONNECT = "dbname=musicbrainz_db user=musicbrainz host=musicbrainz-docker_db_1 port=5432 password=musicbrainz"

ARTIST_CONFIDENCE_THRESHOLD = .45
NUM_ROWS_PER_COMMIT = 2500
MAX_THREADS = 8

class MappingLookupIndex:

    def create(self, conn, index_dir):
        last_row = None
        current_part_id = None

        t0 = monotonic()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
            artist_data = []
            stupid_artist_data = []  # reserved for stupid artists like !!!
            recording_data = []
            release_data = []
            relrec_offsets = []
            relrec_offset = 0

            db_file = os.path.join(index_dir, "mapping.db")
            create_db(db_file)

            print("execute query")
            curs.execute(""" SELECT artist_credit_id
                                  , artist_mbids::TEXT[]
                                  , artist_credit_name
                                  , COALESCE(array_agg(a.sort_name ORDER BY acn.position)) as artist_credit_sortname
                                  , rel.id AS release_id
                                  , rel.gid::TEXT AS release_mbid
                                  , release_name
                                  , rec.id AS recording_id
                                  , rec.gid::TEXT AS recording_mbid
                                  , recording_name
                                  , score
                               FROM mapping.canonical_musicbrainz_data
                               JOIN recording rec
                                 ON rec.gid = recording_mbid
                               JOIN release rel
                                 ON rel.gid = release_mbid
                               JOIN artist_credit_name acn
                                 ON artist_credit_id = acn.artist_credit
                               JOIN artist a
                                 ON acn.artist = a.id
                           GROUP BY artist_credit_id
                                  , artist_mbids
                                  , artist_credit_name
                                  , release_name
                                  , rel.id
                                  , recording_name
                                  , rec.id
                                  , score
                           ORDER BY artist_credit_id""")
#                              WHERE artist_credit_id > 1230420 and artist_credit_id < 1230800
#                              WHERE artist_credit_id < 10000

            print("load data")
            mapping_data = []
            artist_mapping_data = []
            ad = AlphabetDetector()
            index_file = os.path.join(index_dir, "relrec_data.pickle")
            with open(index_file, "wb") as relrec_f:
                for i, row in enumerate(curs):
                    if i == 0:
                        continue

                    if i % 1000000 == 0:
                        print("Indexed %d rows" % i)

                    
                    if last_row is not None and row["artist_credit_id"] != last_row["artist_credit_id"]:

                        # Save artist data for artist index
                        encoded = FuzzyIndex.encode_string(last_row["artist_credit_name"])
                        if encoded:
                            shard_ch = encoded[0]
                            artist_data.append({ "text": encoded,
                                                 "id": last_row["artist_credit_id"],
                                                 "shard_ch": shard_ch })
                            if not ad.only_alphabet_chars(last_row["artist_credit_name"], "LATIN"):
                                encoded = FuzzyIndex.encode_string(last_row["artist_credit_sortname"][0])
                                if encoded:
#                                    print("%-30s %s %-30s %s" % (last_row["artist_credit_name"], shard_ch,
#                                                                 last_row["artist_credit_sortname"][0], encoded[0]))
                                    # 幾何学模様 a                  Kikagaku Moyo c
                                    artist_data.append({ "text": encoded,
                                                         "id": last_row["artist_credit_id"],
                                                         "shard_ch": shard_ch })

                        else:
                            encoded = FuzzyIndex.encode_string_for_stupid_artists(last_row["artist_credit_name"])
                            if not encoded:
                                last_row = row
                                continue
                            shard_ch = "$"
                            stupid_artist_data.append({ "text": encoded, 
                                                        "id": last_row["artist_credit_id"],
                                                        "shard_ch": shard_ch})

                        # Remove duplicate release/id entries
                        release_data = [dict(t) for t in {tuple(d.items()) for d in release_data}]


                        # pickle release/recording data
                        p_release_data = dumps(release_data)
                        p_recording_data = dumps(recording_data)
                        recording_data = []
                        release_data = []
                
                        # Write out the release/recording data and update artist_offsets
                        relrec_data_size = len(p_release_data) + len(p_recording_data)
                        relrec_offsets.append({ "id": last_row["artist_credit_id"],
                                                "offset": relrec_offset,
                                                "length": relrec_data_size,
                                                "shard_ch": shard_ch})

                        relrec_offset += relrec_data_size
                        relrec_f.write(p_release_data)
                        relrec_f.write(p_recording_data)

                        # Go through the collected mapping data for this artist and set shard_ch, then copy to mapping data
                        for am in artist_mapping_data:
                            am[-1] = shard_ch
                            mapping_data.append(am)
                        artist_mapping_data = []

                    artist_mapping_data.append([row["artist_credit_id"], 
                                                ",".join(row["artist_mbids"]),
                                                row["artist_credit_name"],
                                                row["artist_credit_sortname"][0],
                                                row["release_id"],
                                                row["release_mbid"],
                                                row["release_name"],
                                                row["recording_id"],
                                                row["recording_mbid"],
                                                row["recording_name"],
                                                row["score"],
                                                None])
                    recording_data.append({ "text": FuzzyIndex.encode_string(row["recording_name"]) or "",
                                            "id": row["recording_id"],
                                            "release": row["release_id"],
                                            "score": row["score"]})
                    release_data.append({ "text": FuzzyIndex.encode_string(row["release_name"]) or "",
                                          "id": row["release_id"] })

                    last_row = row

                    if len(mapping_data) > NUM_ROWS_PER_COMMIT:
                        Mapping.insert_many(mapping_data, fields=[Mapping.artist_credit_id,
                                            Mapping.artist_mbids,
                                            Mapping.artist_credit_name,
                                            Mapping.artist_credit_sortname,
                                            Mapping.release_id,
                                            Mapping.release_mbid,
                                            Mapping.release_name,
                                            Mapping.recording_id,
                                            Mapping.recording_mbid,
                                            Mapping.recording_name,
                                            Mapping.score,
                                            Mapping.shard_ch]).execute()
                        mapping_data = []

                # dump out the last bits of data
                encoded = FuzzyIndex.encode_string(row["artist_credit_name"])
                if encoded:
                    artist_data.append({ "text": encoded,
                                         "id": row["artist_credit_id"] })
                else:
                    encoded = FuzzyIndex.encode_string_for_stupid_artists(last_row["artist_credit_name"])
                    stupid_artist_data.append({ "text": encoded,
                                               "id": row["artist_credit_id"] })

                p_release_data = dumps(release_data)
                p_recording_data = dumps(recording_data)

                relrec_data_size = len(p_release_data) + len(p_recording_data)
                relrec_offsets.append({ "id": row["artist_credit_id"],
                                        "offset": relrec_offset,
                                        "length": relrec_data_size,
                                        "shard_ch": encoded[0]})

                relrec_f.write(p_release_data)
                relrec_f.write(p_recording_data)

                recording_data = []
                release_data = []

                if mapping_data:
                    Mapping.insert_many(mapping_data, fields=[Mapping.artist_credit_id,
                                        Mapping.artist_mbids,
                                        Mapping.artist_credit_name,
                                        Mapping.artist_credit_sortname,
                                        Mapping.release_id,
                                        Mapping.release_mbid,
                                        Mapping.release_name,
                                        Mapping.recording_id,
                                        Mapping.recording_mbid,
                                        Mapping.recording_name,
                                        Mapping.score,
                                        Mapping.shard_ch]).execute()

                print("Create mapping indexes")
                Mapping.add_index(Mapping.artist_credit_id)
                Mapping.add_index(Mapping.release_id)
                Mapping.add_index(Mapping.recording_id)


        # Sort the relrecs in unidecode space
        relrec_sorted = sorted(relrec_offsets, key=lambda x: (x["shard_ch"], x["id"]))

        print("Write relrec offsets table")
        r_file = os.path.join(index_dir, "relrec_offset_table.binary")
        with open(r_file, "wb") as f:
            for relrec in relrec_sorted:
                f.write(pack("IIIc", relrec["offset"], relrec["length"], relrec["id"], bytes(relrec["shard_ch"], "utf-8")))

        print("Write shard offsets table")
        shard_offsets = {}
        relrec_offsets = sorted(relrec_offsets, key=lambda x: (x["shard_ch"], x["offset"]))
        for r_index, r_offset in enumerate(relrec_offsets):
            relrec_offset = r_index * 13
            if r_offset["shard_ch"] not in shard_offsets:
                shard_offsets[r_offset["shard_ch"]] = {"shard_ch":r_offset["shard_ch"], "offset": relrec_offset, "length": 13}
            else:
                shard_offsets[r_offset["shard_ch"]]["length"] += 13

        shard_table = sorted(shard_offsets.values(), key=lambda x: (x["shard_ch"], x["offset"]))
        for i, s in enumerate(shard_table):
            print(f"{i}: {s['offset']:<12,} {s['length']:<12,} {s['shard_ch']}")

        s_file = os.path.join(index_dir, "shard_table.pickle")
        with open(s_file, "wb") as f:
            dump(shard_table, f)

        print("Build/save artist indexes")
        artist_index = FuzzyIndex(name="artist_index")
        artist_index.build(artist_data, "text")
        artist_index.save(index_dir)

        if stupid_artist_data:
            print("Build/save stupid artist indexes")
            stupid_artist_index = FuzzyIndex(name="stupid_artist_index")
            stupid_artist_index.build(stupid_artist_data, "text")
            stupid_artist_index.save(index_dir)

        t1 = monotonic()
        print("loaded data and build artist indexes in %.1f seconds." % (t1 - t0))


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
