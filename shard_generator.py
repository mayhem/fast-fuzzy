#!/usr/bin/env python3

from collections import defaultdict
import json

from tabulate import tabulate
from icecream import ic

from fuzzy_index import FuzzyIndex
from utils import split_list_evenly, split_dict_evenly


with open("../typesense_queries.txt", "r") as f:
    index = defaultdict(int)
    for line in f.readlines():
        d = json.loads(line)
        encoded = FuzzyIndex.encode_string(d["artist"])
        if encoded:
            index[encoded[0]] += 1

print("original")
for k in sorted(index):
    print(f"{k}: {index[k]:,}")
print()

split_index = split_dict_evenly(index, 5)
ic(split_index)

for i, l in enumerate(split_index):
    print(f"shard {i}:")
    total = 0
    for k in l:
        print(f"  {k} {l[k]:,}")
        total += l[k]
    print("total requests per shard %d" % total)

print("PYTHON CODE FOLLOWS: (save to shards.py))")

print("shard_table = [")
for l in split_index:
    print("  ", l, end=",\n")
print("]")
