import os
from math import fabs
from time import monotonic
import pickle
import re
import sys

import sklearn
from sklearn.feature_extraction.text import TfidfVectorizer

import nmslib
from unidecode import unidecode
from utils import ngrams

MAX_ENCODED_STRING_LENGTH = 30


class FuzzyIndex:
    '''
       Create a fuzzy index using a Term Frequency, Inverse Document Frequency (tf-idf)
       algorithm. Currently the libraries that implement this cannot be serialized to disk,
       so this is an in memory operation. Fortunately for our amounts of data, it should
       be quick to rebuild this index.
    '''

    def __init__(self, name=None):
        self.index_data = None
        self.name = name
        self.index = nmslib.init(method='simple_invindx', space='negdotprod_sparse_fast', data_type=nmslib.DataType.SPARSE_VECTOR)
        self.vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)

    @staticmethod
    def encode_string(text):
        """Remove spaces, punctuation, convert non-ascii characters to some romanized equivalent, lower case, return"""
        if text is None:
            return None
        return unidecode(re.sub("[ _]+", "", re.sub(r'[^\w ]+', '', text)).strip().lower())[:MAX_ENCODED_STRING_LENGTH]

    @staticmethod
    def encode_string_for_stupid_artists(text):
        """Remove spaces, convert non-ascii characters to some romanized equivalent, lower case, return"""
        if text is None:
            return None
        return unidecode(re.sub("[ _]+", "", text).strip())[:MAX_ENCODED_STRING_LENGTH]

    def build(self, index_data, field):
        if not index_data:
            raise ValueError("No index data passed to index build().")
        self.index_data = index_data
        strings = [x[field] for x in index_data]
        lookup_matrix = self.vectorizer.fit_transform(strings)
        self.index.addDataPointBatch(lookup_matrix, list(range(len(strings))))
        self.index.createIndex()

    def save(self, index_dir):
        v_file = os.path.join(index_dir, "%s_nmslib_vectorizer.pickle" % self.name)
        i_file = os.path.join(index_dir, "%s_nmslib_index.pickle" % self.name)
        d_file = os.path.join(index_dir, "%s_additional_index_data.pickle" % self.name)

        with open(v_file, "wb") as f:
            pickle.dump(self.vectorizer, f)
        self.index.saveIndex(i_file, save_data=True)
        with open(d_file, "wb") as f:
            pickle.dump(self.index_data, f)

    def load(self, index_dir):
        v_file = os.path.join(index_dir, "%s_nmslib_vectorizer.pickle" % self.name)
        i_file = os.path.join(index_dir, "%s_nmslib_index.pickle" % self.name)
        d_file = os.path.join(index_dir, "%s_additional_index_data.pickle" % self.name)

        try:
            with open(v_file, "rb") as f:
                self.vectorizer = pickle.load(f)
            self.index.loadIndex(i_file, load_data=True)
            with open(d_file, "rb") as f:
                self.index_data = pickle.load(f)
            return True
        except OSError:
            return False


    def search(self, query_string, min_confidence, debug=False):
        """ Carry out search, returns list of dicts: "text", "id", "confidence" """

        if self.index is None:
            raise IndexError("Must build index before searching")

        query_matrix = self.vectorizer.transform([query_string])
        # TOTUNE: k might need tuning
        results = self.index.knnQueryBatch(query_matrix, k=15, num_threads=5)
        output = []
        if debug:
            print("Search results for '%s':" % query_string)
        for i, conf in zip(results[0][0], results[0][1]):
            data = self.index_data[i]
            confidence = fabs(conf)
            if confidence >= min_confidence:
                data["confidence"] = fabs(conf)
                output.append(data)
                if debug:
                    print("  %-30s %10d %.3f" % (data["text"][:30], data["index"], data["confidence"]))

        return output
