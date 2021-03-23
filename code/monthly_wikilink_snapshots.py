import gzip
from pathlib import Path
from typing import Any
from collections.abc import Iterable, Mapping
from itertools import chain, groupby
from datetime import datetime
import pandas as pd
from mw.types.timestamp import Timestamp as wmtimestamp
from dataclasses import dataclass
import pyarrow as pa
import pyarrow.parquet as pq
import csv
import glob

header = b'page_id,page_title,revision_id,revision_parent_id,revision_timestamp,user_type,user_username,user_id,revision_minor,wikilink.link,wikilink.tosection,wikilink.anchor,wikilink.section_name,wikilink.section_level,wikilink.section_number'

keys = header.decode().split(',')

def split_line(line: bytes) -> Iterable[str]:
    items = line.decode().split(',')
    return items 

def try_int(s):
    try:
        return int(s)
    except ValueError:
        return None

@dataclass
class Wikilink:
    page_id:int
    page_title:str
    revision_id:int
#    revision_parent_id:int
    revision_timestamp:datetime
    wikilink:str

    @staticmethod
    def from_line(csv_line:bytes):
        try:
            revision_timestamp = datetime.fromtimestamp(wmtimestamp(csv_line[4]).serialize())
            
            obj = Wikilink(page_id = int(csv_line[0]),
                           page_title = csv_line[1],
                           revision_id = int(csv_line[2]),
                           #                       revision_parent_id = try_int(items[3]),
                           revision_timestamp = revision_timestamp,
                           wikilink = csv_line[9])
        except ValueError:
            print(line)

        return obj

    def get_month(self):
        month = self.revision_timestamp.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
        return month

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

def lines_from_paths(files: Iterable[Path]) -> Iterable[str]:
    lines = chain(* map(lines_from_gzip, files))
    return lines

def wikilinks_from_lines(lines:Iterable[bytes]) -> Iterable[Wikilink]:
    return map(Wikilink.from_line, lines)



#test_path = Path("/home/nathante/mnt/wikilinks/enwiki-20180301-pages-meta-history10.xml-p2369541p2403290.7z.rawwikilinks.csv.gz")

# great now we are reading a set of files one line at a time.
# the next thing to do is to find the version that was current on the first of the months.
# the easist way to do this is to group by page_id
#lines = lines_from_paths([test_path)

lines = lines_from_paths(glob.glob("/data/wikilinks/*.csv.gz"))

wikilinks = wikilinks_from_lines(lines)

def last_in_month(wikilinks):
    wikilinks_by_month = groupby(wikilinks, key = lambda wl: (wl.page_id, wl.get_month()))
    for _, page_revs in wikilinks_by_month:
        last = None
        for rev in page_revs: 
            if last is None:
                last = rev
            elif rev.revision_timestamp > last.revision_timestamp:
                last = rev
            else:
                continue
        yield last

def group_by_page(wikilinks):
    return groupby(wikilinks, key = lambda wl: wl.page_id)

last_wikilinks_in_month = last_in_month(wikilinks)

page_revisions = group_by_page(last_wikilinks_in_month)

def monthly_links(page_revisions):
    df = pd.DataFrame(page_revisions)
    if df.shape[0] == 0:
        return None

    revisions = df.loc[:,['revision_id','revision_timestamp']].drop_duplicates().reset_index(drop=True)
    first_date = revisions.revision_timestamp.min().round("1 D")
    last_date = revisions.revision_timestamp.max().round("1 D")

    months = pd.date_range(first_date,last_date,freq='M').to_series()
    months = months.append(pd.Series(months.max() + pd.offsets.MonthEnd(1)))

    if months.shape[0] == 1:
        last_date = first_date + pd.offsets.MonthEnd(1)
        months = pd.date_range(first_date,last_date,freq='M').to_series()

    else:
        months = pd.date_range(first_date,last_date,freq='M').to_series()
        months = months.append(pd.Series(months.max() + pd.offsets.MonthEnd(1)))

    months = pd.DataFrame({"months":months,"revision_timestamp":months})
    #    revisions.set_index("revision_timestamp",inplace=True)

    if any(months.isna().any()):
        print(df)

    revisions = revisions.sort_values("revision_timestamp")

    revisions = pd.merge_asof(revisions,months,direction='forward')

    last_revisions = revisions.groupby("months").min()
    last_revisions = last_revisions.drop("revision_timestamp",1)
    last_revisions = last_revisions.set_index("revision_id")

    df.set_index("revision_id",inplace=True)
    df = df.join(last_revisions,how='inner')
    df = df.groupby(['revision_id','revision_timestamp','page_id','page_title'])['wikilink'].apply(list)
    df = df.reset_index(drop=False)
    return df

outparquet = Path("/mnt/wikilinks/monthly_link_snapshot.parquet")

pqwriter = None
for page_id, page_revs in page_revisions:
    df = monthly_links(page_revs)
    table = pa.Table.from_pandas(df)
    if not pqwriter:
        pqwriter = pq.ParquetWriter(outparquet, table.schema)

    pqwriter.write_table(table)

# def dict_from_line(line: bytes, keys: Iterable[str]) -> Mapping[str, Any]:
#     items = split_line(line)
#     return dict(zip(keys,items))

# def dicts_from_lines(lines:Iterable[bytes], keys:list) -> Iterable[Mapping[str, Any]]:
#     partial_dict_from_line = lambda l: dict_from_line(l, keys=keys)
#     return map(partial_dict_from_line, lines)

# def dicts_from_path(path:Path, keys:Iterable[str]) -> Iterable[Mapping[str, Any]]:
#     lines = lines_from_gzip(path)
#     return dicts_from_lines(lines, keys)

# def dicts_from_paths(files: Iterable[Path],keys=Iterable[str]) -> Iterable[Mapping[str, Any]]:
#     partial_dicts_from_path = lambda p: dicts_from_path(p, keys=keys)
#     return chain(* map(partial_dicts_from_path, files))
    

# def pages_from_lines(group: Iterable[bytes]) -> pd.DataFrame:
#     items = map(group, split_line)
#     df = pd.DataFrame(items, columns=keys)

