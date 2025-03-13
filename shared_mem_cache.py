from time import monotonic, sleep
from multiprocessing import shared_memory
import glob
import pickle
import os
import sys

from cache import ArtistDataCache
from fuzzy_index import FuzzyIndex

# How much to reduce the cached data by when it gets near the limit
CACHE_PURGE_SIZE_REDUCTION = 10  # in %
# When to start reducing the in memory cache
CACHE_PURGE_THRESHOLD = 90  # in %

def start_manager_thread(obj):
    obj.cache_manager_thread()

class SharedMemoryArtistDataCache(ArtistDataCache):
    """class for caching data using shared memory"""

    def __init__(self, temp_dir, max_cache_size):
        self.temp_dir = temp_dir
        self.exit = False
        self.max_cache_size = max_cache_size
        
    def stop_process(self):
        self.exit = True
        
    def pickle_data(self, artist_data):
        if artist_data is None or artist_data["release_index"] is None or artist_data["recording_index"] is None:
            return pickle.dumps("[empty]")

        prepared = {
            "recording_data": artist_data["recording_data"],
            "recording_releases": artist_data["recording_releases"],
            "release_data": artist_data["release_data"],
            "release_index": artist_data["release_index"].save_to_mem(self.temp_dir) if artist_data is not None else None,
            "recording_index": artist_data["recording_index"].save_to_mem(self.temp_dir) if artist_data is not None else None
        }
        return pickle.dumps(prepared)
        
    def save(self, artist_id, artist_data):
        pickled = self.pickle_data(artist_data)
        p_len = len(pickled)
        try:
            shm = shared_memory.SharedMemory(name=f"a{artist_id}", create=True, size=p_len, track=False)
            shm.buf[:p_len] = bytearray(pickled)
        except FileExistsError:
            pass
        except TypeError:
            print(artist_data)
            print("p: '%s'" % pickled)
            print("plen: '%s'" % p_len)
        
    def load(self, artist_id):
        try:
            shm = shared_memory.SharedMemory(name=f"a{artist_id}", create=False, track=False)
        except FileNotFoundError:
            return None

        return self.unpickle_data(shm.buf)
    
    def unpickle_data(self, data):

        pickled = pickle.loads(data)
        if pickled == "[empty]":
            return {
                "recording_data": None,
                "recording_releases": None,
                "release_data": None,
                "release_index": None,
                "recording_index": None,
            }
        fi = FuzzyIndex()
        fi.load_from_mem(pickled["release_index"], self.temp_dir)
        pickled["release_index"] = fi
        fi = FuzzyIndex()
        fi.load_from_mem(pickled["recording_index"], self.temp_dir)
        pickled["recording_index"] = fi 
        
        return pickled
    
    def clear_cache(self):
        print("clear artist cache")
        for f in os.listdir("/dev/shm"):
            filename = os.path.basename(f)
            try:
                shm = shared_memory.SharedMemory(name=filename, create=False, track=False)
            except OSError as err:
                print("Cannot removed shared memory block %s:" % filename, str(err))
                continue
            shm.close()
            shm.unlink()

    def cache_manager_thread(self):
        while not self.exit:
            # run the cache check once a minute
            sleep(30)

            index = {}
            files = list(filter(os.path.isfile, glob.glob("/dev/shm/" + "*")))
            for filename in files:
                try:    
                    index[filename] = (os.path.getsize(filename), os.path.getatime(filename))
                except FileNotFoundError:
                    pass

            total_size = sum([ index[x][0] for x in index ])
            if total_size < (self.max_cache_size * CACHE_PURGE_THRESHOLD / 100):
                print("not cleaning cache: %d < %d" % (total_size, (self.max_cache_size * CACHE_PURGE_THRESHOLD / 100)))
                continue

            target_size = self.max_cache_size * CACHE_PURGE_SIZE_REDUCTION / 100
            print("target size: %d total size: %d" %(target_size, total_size)) 
            for filename in sorted(files, key=lambda x: index[x][1]):
                base = os.path.basename(filename)
                try:
                    shm = shared_memory.SharedMemory(name=base, create=False, track=False)
                except OSError as err:
                    print("Cannot removed shared memory block %s:" % base, str(err))
                    continue
                shm.close()
                shm.unlink()
                
                total_size -= index[filename][0]
                if total_size < target_size:
                    break