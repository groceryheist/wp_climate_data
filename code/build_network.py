import pandas as pd
import csv
import gzip
from pathlib import Path
from collections.abc import Iterable, Mapping
import numpy as np
import glob
import os


header = ["page_id_from","page_title_from","page_id_to","page_title_to","N_hops"]

def split_line(line: bytes) -> Iterable[str]:
    items = line.decode().split(',')
    return items 

def try_int(s):
    try:
        return int(s)
    except ValueError:
        return None

def lines_from_gzip(path: Path) -> Iterable[bytes]:
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


def expand_graph(graph:set, distances:Mapping, infile:Path, outfile:Path):
    print(graph)
    new_nodes = set()
    wd = os.getcwd()
    os.chdir(outfile.parent)
    with gzip.open(str(outfile.name),"wt") as of:
        writer = csv.writer(of,dialect='excel-tab')
        writer.writerow(header)
        for line in lines_from_gzip(Path(infile)):
            dest_id = try_int(line[2])
            source_id = try_int(line[0])
            if dest_id in graph: # we're connected
                new_nodes.add(source_id)
                dist = distances[dest_id]+1
                line.append(dist)
                distances[source_id] = int(np.min([distances.get(source_id,np.Inf),dist]))
                writer.writerow(line)
    os.chdir(wd)
    return (new_nodes.union(graph), distances)

# build a N-hop-network
def N_hop_network(nodes, distances, infile, outfile, N):
    for i in range(N):
        if i < (N-1):
            step_outfile = outfile.parent / f"{i}_{outfile.name}"
        else:
            step_outfile = outfile
        nodes, distances = expand_graph(nodes, distances, infile, step_outfile)
    return nodes


# build a three_hop network starting with climate change
# infile="../data/wikilinkgraphs/enwiki.snapshot.resolve_redirect.2005-03-01.csv.gz";
# outfile="climate-3hop.wikilink_graph.2005-10-15.csv.gz";
# N=3;
if __name__ == "__main__":
    initial_graph = {6266,5042951}
    distances = {6266:0,5042951:0}
    
    base_path = Path("../data")
    files = [Path(p) for p in glob.glob(str(base_path / "enwiki.wikilink_graph.*.csv.gz"))]

    for infile in files:
        outfile = infile.parent / (infile.stem[:-4] + "-climate_network.csv.gz" )

        N_hop_network(initial_graph, distances, infile, outfile, 4)
