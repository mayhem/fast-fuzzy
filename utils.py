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
