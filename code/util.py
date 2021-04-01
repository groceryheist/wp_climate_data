import gzip
from pathlib import Path
from collections.abc import Iterable

def split_line(line: bytes) -> Iterable[str]:
    items = line.decode().split(',')
    return items 

def try_int(s):
    try:
        return int(s)
    except ValueError:
        return None

def lines_from_gzip(path: Path) -> Iterable[bytes]:
    lines = gzip.open(path,'rb')
    # throw away the the header
    header = next(lines)
    # convert to string
    lines = (s.decode() for s in lines)
    print(path)
    try:
        yield from csv.reader(lines)
    except csv.Error:
        print(f"Error in {path}")
