from multiprocessing import Pool
from pathlib import Path
from collections.abc import Iterable
import csv
import gzip
import glob
import re

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

def count_edges(infile):
    count = 0
    for line in lines_from_gzip(Path(infile)):
        count = count + 1
    return count

if __name__ == "__main__":
    base_path = Path("../data")
    files = [Path(p) for p in glob.glob(str(base_path / "enwiki.wikilink_graph.*.csv.gz"))][0:2]
    header = ["date","N_wikilinks"]
    with open("num_wikilinks.csv",'wt') as buf:
        of = csv.writer(buf, dialect='excel-tab')
        of.writerow(header)
        pool = Pool(8)
        counts = pool.imap(count_edges,files)
        dates = map(lambda infile: re.findall(r"(\d{4}-\d{2}-\d{2})",str(infile))[0], files)
        z = zip(counts,dates)
        for count, date in z:
            of.writerow([date,count])
            buf.flush()
