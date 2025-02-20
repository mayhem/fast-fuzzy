#!/usr/bin/env python3

import csv
import os
import datetime
from math import fabs
from time import monotonic
import threading
from queue import Queue
import pickle
import re
import sys
import joblib

import psycopg2
from psycopg2.extras import DictCursor, execute_values

import sklearn
from sklearn.feature_extraction.text import TfidfVectorizer
from joblib import parallel_backend

import nmslib
from unidecode import unidecode

# For wolf
DB_CONNECT = "dbname=musicbrainz_db user=musicbrainz host=localhost port=5432 password=musicbrainz"
ARTIST_CONFIDENCE_THRESHOLD = .45
CHUNK_SIZE = 100000
MAX_THREADS = 8

# TOTUNE
MAX_ENCODED_STRING_LENGTH = 30

def ngrams(string, n=3):
    """ Take a lookup string (noise removed, lower case, etc) and turn into a list of trigrams """

    string = ' ' + string + ' '  # pad names for ngrams...
    ngrams = zip(*[string[i:] for i in range(n)])
    return [''.join(ngram) for ngram in ngrams]

class IndexDataPickleList(list):
    def __getstate__(self):
        state = self.__dict__.copy()
        try:
            # Don't pickle index
            del state["index"]
        except KeyError:
            pass
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.index = None


class FuzzyIndex:
    '''
       Create a fuzzy index using a Term Frequency, Inverse Document Frequency (tf-idf)
       algorithm. Currently the libraries that implement this cannot be serialized to disk,
       so this is an in memory operation. Fortunately for our amounts of data, it should
       be quick to rebuild this index.
    '''

    def __init__(self):
        self.index_data = None
        self.index = nmslib.init(method='simple_invindx',
                                 space='negdotprod_sparse_fast',
                                 data_type=nmslib.DataType.SPARSE_VECTOR)
        self.vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)

    @staticmethod
    def encode_string(text):
        if text is None:
            return None
        return unidecode(re.sub(" +", "", re.sub(r'[^\w ]+', '', text)).strip().lower())[:MAX_ENCODED_STRING_LENGTH]

    def build(self, index_data, field):
        self.index_data = index_data
        strings = [ x[field] for x in index_data ]
        lookup_matrix = self.vectorizer.fit_transform(strings)
        self.index.addDataPointBatch(lookup_matrix, list(range(len(strings))))
        self.index.createIndex()

    def save(self, index_dir):
        v_file = os.path.join(index_dir, "vectorizer.pickle")
        i_file = os.path.join(index_dir, "nmslib_index.pickle")
        d_file = os.path.join(index_dir, "index_data.pickle")

        pickle.dump(self.vectorizer, open(v_file, "wb"))
        self.index.saveIndex(i_file, save_data=True)
        pickle.dump(self.index_data, open(d_file, "wb"))

    def load(self, index_dir):
        v_file = os.path.join(index_dir, "vectorizer.pickle")
        i_file = os.path.join(index_dir, "nmslib_index.pickle")
        d_file = os.path.join(index_dir, "index_data.pickle")

        self.vectorizer = pickle.load(open(v_file, "rb"))
        self.index.loadIndex(i_file, load_data=True)
        self.index_data = pickle.load(open(d_file, "rb"))

    def search(self, query_string):
        if self.index is None:
            raise IndexError("Must build index before searching")

        query_matrix = self.vectorizer.transform([query_string])
        # TOTUNE: k might need tuning
        results = self.index.knnQueryBatch(query_matrix, k=15, num_threads=5)

        output = []
        for i, conf in zip(results[0][0], results[0][1]):
            data = self.index_data[i]
            data["confidence"] = fabs(conf)
            output.append(data)

        return output


class MappingLookupIndex:

    def __init__(self):
        self.artist_data = IndexDataPickleList()
        self.return_value_queue = Queue()

    def create(self, conn):

        # TODO: VA and more complex artist credits probably not handled correctly
        self.artist_index = FuzzyIndex()

        last_artist_credit_id = -1
        last_row = None

        t0 = monotonic()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
            recording_data = []

            print("execute query")
            curs.execute(""" SELECT artist_credit_id
                                  , artist_mbids
                                  , artist_credit_name
                                  , release_name
                                  , release_mbid
                                  , recording_name
                                  , recording_mbid
                               FROM mapping.canonical_musicbrainz_data
                               WHERE artist_credit_id < 100000
                               ORDER BY artist_credit_id""")

            print("load data")
            for i, row in enumerate(curs):
                if i == 0:
                    continue

                if i % 1000000 == 0:
                    print("Indexed %d rows" % i)

                if last_artist_credit_id >= 0 and row["artist_credit_id"] != last_artist_credit_id:
                    self.artist_data.append({ "artist_mbids": last_row["artist_mbids"],
                                              "artist_credit_name": last_row["artist_credit_name"],
                                              "text": FuzzyIndex.encode_string(last_row["artist_credit_name"]),
                                              "index": None,
                                              "recording_data": recording_data})
                    recording_data = []

                recording_data.append({ "text": FuzzyIndex.encode_string(row["recording_name"]) or "",
                                        "recording_mbid": row["recording_mbid"],
                                        "recording_name": row["recording_name"] })
                last_row = row
                last_artist_credit_id = row["artist_credit_id"]

        self.artist_index.build(self.artist_data, "text")

        t1 = monotonic()
        print("loaded data and build artist index in %.1f seconds." % (t1 - t0))

    def save(self, index_dir):
        self.artist_index.save(index_dir)

    def load(self, index_dir):
        self.artist_index = FuzzyIndex()
        self.artist_index.load(index_dir)

    def search(self, artist_name, recording_name):

        # First do artist fuzzy search, which takes 1-2ms with a full index.
        artist_name = FuzzyIndex.encode_string(artist_name)
        recording_name = FuzzyIndex.encode_string(recording_name)

        t0 = monotonic()
        artists = self.artist_index.search(artist_name)
        t1 = monotonic()
        print("artist index search: %.1fms" % ((t1-t0)*1000))
        results = []

        # For each hit, search recordings.
        for artist in artists:
            if artist["confidence"] > ARTIST_CONFIDENCE_THRESHOLD:
                print("search recordings for: '%s' " % artist["artist_credit_name"], artist["artist_mbids"])

                # Fetch the index for the recordings -- if not built yet, build it!
                if artist["index"] is None:
                    index = FuzzyIndex()
                    index.build(artist["recording_data"], "text")
                    artist["index"] = index

                rec_results = artist["index"].search(recording_name)
                for result in rec_results:
                    results.append({ "artist_name": artist["text"],
                                     "artist_mbids": artist["artist_mbids"],
                                     "artist_confidence": artist["confidence"],
                                     "recording_name": result["recording_name"],
                                     "recording_mbid": result["recording_mbid"],
                                     "recording_confidence": result["confidence"] })
            else:
                print("Artist '%s' %.1f ignored" % (artist["text"], artist["confidence"]))

        return sorted(results, key=lambda a: (a["artist_confidence"], a["recording_confidence"]), reverse=True)


mi = MappingLookupIndex()
if sys.argv[1] == "build":
    sklearn.set_config(working_memory=1024 * 100)
    with parallel_backend('threading', n_jobs=MAX_THREADS):
        with psycopg2.connect(DB_CONNECT) as conn:
            mi.create(conn)
            mi.save("index")
else:
    mi.load("index")
    while True:
        query = input("artist,recording>")
        if not query:
            continue
        try:
            artist_name, recording_name = query.split(",")
        except ValueError:
            print("Input must be artist then recording, separated by comma")
            continue

        t0 = monotonic()
        results = mi.search(artist_name, recording_name)
        t1 = monotonic()
        for result in results:
            print("%-40s %.3f %s %-40s %.3f %s" % (result["artist_name"],
                                                    result["artist_confidence"],
                                                    result["artist_mbids"],
                                                    result["recording_name"],
                                                    result["recording_confidence"],
                                                    result["recording_mbid"]))
            
        print("%.1fms total search time" % ((t1 - t0) * 1000))
