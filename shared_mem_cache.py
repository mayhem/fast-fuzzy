from time import monotonic, sleep
from multiprocessing import shared_memory
import pickle
import os
import sys

from cache import ArtistDataCache
from fuzzy_index import FuzzyIndex

class SharedMemoryArtistDataCache(ArtistDataCache):
    """class for caching data using shared memory"""

    def __init__(self, temp_dir):
        self.temp_dir = temp_dir
        
    def pickle_data(self, artist_data):
        prepared = {
            "recording_data": artist_data["recording_data"],
            "recording_releases": artist_data["recording_releases"],
            "release_data": artist_data["release_data"],
            "release_index": artist_data["release_index"].save_to_mem(self.temp_dir),
            "recording_index": artist_data["recording_index"].save_to_mem(self.temp_dir)
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
        
    def load(self, artist_id):
        try:
            shm = shared_memory.SharedMemory(name=f"a{artist_id}", create=False, track=False)
        except FileNotFoundError:
            return None

        return self.unpickle(shm.buf)
    
    def unpickle_data(self, data):
        pickled = pickle.loads(data)
        fi = FuzzyIndex()
        fi.load_from_mem(pickled["release_index"], self.temp_dir)
        pickled["release_index"] = fi
        fi = FuzzyIndex()
        fi.load_from_mem(pickled["recording_index"], self.temp_dir)
        pickled["recording_index"] = fi 
        
        return pickled
    
    def clear_cache(self):
        for f in os.listdir("/dev/shm"):
            try:
                shm = shared_memory.SharedMemory(name=f, create=False)
            except OSError as err:
                print("Cannot removed shared mempory block %s:" % f, str(err))
                continue
            shm.close()
            shm.unlink()

    @staticmethod 
    def cache_manager_thread():
        pass
        