#!/usr/bin/env python3

import csv
import os
import datetime
from math import fabs
from time import monotonic
import threading
from queue import Queue
import re
import sys

import psycopg2
from psycopg2.extras import DictCursor, execute_values

import sklearn
from sklearn.feature_extraction.text import TfidfVectorizer
from joblib import parallel_backend

import nmslib
from unidecode import unidecode

# For wolf
DB_CONNECT = "dbname=musicbrainz_db user=musicbrainz host=localhost port=5432 password=musicbrainz"
ARTIST_CONFIDENCE_THRESHOLD = .7
CHUNK_SIZE = 100000
MAX_THREADS = 8

# TOTUNE
MAX_ENCODED_STRING_LENGTH = 30

def ngrams(string, n=3):
    """ Take a lookup string (noise removed, lower case, etc) and turn into a list of trigrams """

    string = ' ' + string + ' '  # pad names for ngrams...
    ngrams = zip(*[string[i:] for i in range(n)])
    return [''.join(ngram) for ngram in ngrams]


class FuzzyIndex:
    '''
       Create a fuzzy index using a Term Frequency, Inverse Document Frequency (tf-idf)
       algorithm. Currently the libraries that implement this cannot be serialized to disk,
       so this is an in memory operation. Fortunately for our amounts of data, it should
       be quick to rebuild this index.
    '''

    def __init__(self):
        self.index = None
        self.vectorizer = None

    @staticmethod
    def encode_string(text):
        if text is None:
            return None
        return unidecode(re.sub(" +", "", re.sub(r'[^\w ]+', '', text)).strip().lower())[:MAX_ENCODED_STRING_LENGTH]

    def build(self, search_data):
        """
            Builds a new index and saves it to disk and keeps it in ram as well.
        """

        lookup_strings = []
        lookup_ids = []
        for value, lookup_id in search_data:
            lookup_strings.append(value)
            lookup_ids.append(lookup_id)

        self.vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)

        # The fit_transform function carries out disk writes, which slows things way down.
        # How does one ensure that this is a fully in-memory operation?
        lookup_matrix = self.vectorizer.fit_transform(lookup_strings)

        self.index = nmslib.init(method='simple_invindx',
                                 space='negdotprod_sparse_fast',
                                 data_type=nmslib.DataType.SPARSE_VECTOR)
        self.index.addDataPointBatch(lookup_matrix, lookup_ids)
        self.index.createIndex()

    def search(self, query_string):
        """
            Return IDs for the matches in a list. Returns a list of dicts with keys of lookup_string, confidence and recording_id.
        """
        if self.index is None:
            raise IndexError("Must build index before searching")

        query_matrix = self.vectorizer.transform([query_string])
        # TOTUNE: k might need tuning
        results = self.index.knnQueryBatch(query_matrix, k=15, num_threads=5)

        output = []
        for index, conf in zip(results[0][0], results[0][1]):
            output.append({"confidence": fabs(conf), "id": index, "text": query_string })

        return output


class MappingLookupIndex:

    def __init__(self):
        self.artist_data = {}
        self.return_value_queue = Queue()

    def create_indexes(self, conn):

        # TODO: VA and more complex artist credits probably not handled correctly
        self.artist_index = FuzzyIndex()
        self.recording_indexes = {}

        last_artist_credit_id = -1
        last_row = None

        t0 = monotonic()
        # Read from CSV file, since no sort, faster to iterate
        with open('canonical_musicbrainz_data.csv', newline='') as csvfile:
            reader = csv.reader(csvfile)

            recording_data = []
            for i, csv_row in enumerate(reader):
                if i == 0:
                    continue

                if i % 1000000 == 0:
                    print("Indexed %d rows" % i)

                # Make the data look like it came from PG
                row = { "id": int(csv_row[0]),
                        "artist_credit_id": int(csv_row[1]),
                        "artist_credit_name": csv_row[3],
                        "recording_name": csv_row[7]
                      }

                if last_artist_credit_id >= 0 and row["artist_credit_id"] != last_artist_credit_id:
                    self.recording_indexes[last_artist_credit_id] = recording_data
                    recording_data = []
                    self.artist_data[last_row["artist_credit_id"]] = (FuzzyIndex.encode_string(last_row["artist_credit_name"]),
                                                                                               last_row["artist_credit_id"])

                encoded = FuzzyIndex.encode_string(row["recording_name"])
                if encoded:
                    recording_data.append((encoded, row["id"]))
                last_row = row
                last_artist_credit_id = row["artist_credit_id"]

        self.artist_index.build(self.artist_data.values())
        t1 = monotonic()
        print("loaded data and build artist index in %.1f seconds." % (t1 - t0))

    def search(self, artist_name, recording_name):

        # First do artist fuzzy search, which takes 1-2ms with a full index.
        artist_name = FuzzyIndex.encode_string(artist_name)
        recording_name = FuzzyIndex.encode_string(recording_name)

        t0 = monotonic()
        artists = self.artist_index.search(artist_name)
        t1 = monotonic()
        print("artist index search: %.1fms" % ((t1-t0)*1000)
        results = []

        # For each hit, search recordings.
        for artist in artists:
            artist["id"] = int(artist["id"])
            artist["text"] = self.artist_data[artist["id"]][0]
            if artist["confidence"] > ARTIST_CONFIDENCE_THRESHOLD:
                print("search recordings for: '%s'" % artist["text"])

                # Fetch the index for the recordings -- if not built yet, build it!
                search_index = self.recording_indexes[artist["id"]]
                if not isinstance(search_index, FuzzyIndex):
                    fi = FuzzyIndex()
                    fi.build(search_index)
                    self.recording_indexes[artist["id"]] = fi
                    search_index = fi

                rec_results = search_index.search(FuzzyIndex.encode_string(recording_name))
                for result in rec_results:
                    results.append({ "artist_name": artist["text"],
                                     "artist_credit_id": artist["id"],
                                     "artist_confidence": artist["confidence"],
                                     "recording_name": result["text"],
                                     "canonical_id": result["id"],
                                     "recording_confidence": result["confidence"] })

        return results


mi = MappingLookupIndex()

sklearn.set_config(working_memory=1024 * 100)
with parallel_backend('threading', n_jobs=MAX_THREADS):
    with psycopg2.connect(DB_CONNECT) as conn:
        mi.create_indexes(conn)
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
                print("%-40s %.3f %6d %-40s %.3f %6d" % (result["artist_name"],
                                                         result["artist_confidence"],
                                                         result["artist_credit_id"],
                                                         result["recording_name"],
                                                         result["recording_confidence"],
                                                         result["canonical_id"]))
                
            print("%.1fms total search time" % ((t1 - t0) * 1000))
