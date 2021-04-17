from collections.abc import Iterable
from pathlib import Path
import pandas as pd
import gzip
import csv
from multiprocessing import Pool

def lines_from_gzip(path: Path) -> Iterable:
    lines = gzip.open(path,'rt')
    # throw away the the header
    header = next(lines)
    # convert to string
#    lines = (s.decode() for s in lines)
    print(path)
    try:
        yield from csv.reader(lines,dialect='excel-tab')
    except csv.Error:
        print(f"Error in {path}")

def count_edges(year):
    lines = lines_from_gzip(f"../data/enwiki.wikilink_graph.{year}-03-01.csv.gz")
    count = 0
    count_climate = 0 
    targets = ['6266','5042951']
    for line in lines:
        if line[2] in targets:
            count_climate = count_climate + 1
        count = count + 1
    return {'year':year,'N':count,'N_climate':count_climate}

pool = Pool(4)
rows  = list(pool.map(count_edges, (str(y) for y in range(2001,2019))))
df = pd.DataFrame(rows)
df['prop_climate'] = df.N_climate / df.N
df.to_csv("../data/wikilink_counts.csv")
