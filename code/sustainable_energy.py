from mw import api
from datetime import datetime 
from mw.types.timestamp import Timestamp
from itertools import chain, islice
from functools import partial, cache
import re
import pandas as pd
from dataclasses import dataclass
from multiprocessing import Pool
from wikidata.client import Client

def parse_wikimedia_timestamp(timestamp):
    return datetime.fromtimestamp(Timestamp(timestamp).serialize())

apiurl = "https://en.wikipedia.org/w/api.php"
session = api.Session(apiurl)
wdclient = Client()
title = "Sustainable energy"

wikilink_re = re.compile(
    r'''\[\[                              # Match two opening brackets
       (?P<link>                          # <link>:
           [^\n\|\]\[\#\<\>\{\}]{0,256}   # Text inside link group
                                          # everything not illegal, non-greedy
                                          # can be empty or up to 256 chars
       )
       (?:                                # Non-capturing group
          \|                              # Match a pipe
          (?P<anchor>                     # <anchor>:
              [^\[]*?                     # Test inside anchor group:
                                          # match everything not an open braket
                                          # - non greedy
                                          # if empty the anchor text is link
          )
       )?                                 # anchor text is optional
       \]\]                               # Match two closing brackets
     ''', re.VERBOSE | re.MULTILINE)

def links_from_rev(rev):
    print(rev['revid'])
    matches = wikilink_re.finditer(rev.get("*",""))
    for match in matches:
        link = match.group("link") or ""
        link = link.strip()
        anchor = match.group("anchor") or link
        anchor = anchor.replace("\n", " ").strip()
        yield {'revid':rev['revid'], 'timestamp':parse_wikimedia_timestamp(rev['timestamp']), 'wikilink':anchor}

def get_monthly_revs(session, page):
    last_month = None
    monthly_revids = []
    all_revs = session.revisions.query(titles={page},properties={'ids','timestamp'},direction='newer')
    for rev in all_revs:
        timestamp = datetime.fromtimestamp(Timestamp(rev['timestamp']).serialize())        
        month = timestamp.replace(day=1,hour=1,minute=1,second=1).date()
        if last_month is None or last_month < month:
            last_month = month
            monthly_revids.append(rev['revid'])

    monthly_revids.reverse()
    
    monthly_revs =  map(lambda revid: session.revisions.get(revid, properties={'ids','timestamp','content'}), monthly_revids)

    yield from monthly_revs

def links_from_article(session, title):
    monthly_revs = get_monthly_revs(session, title)
    yield from chain(* map(links_from_rev, monthly_revs))

def enumerate_subclasses(wdclient, entityid):
    entity = wdclient.get(entityid)
    if entity.id is None:
        return

    subclassof = entity.attributes.get('claims',{}).get('P279',[])
    for cls in subclassof:
        if cls is None:
            continue
        clsid = cls.get("mainsnak",{}).get('datavalue',{}).get("value",{}).get('id',None)
        if clsid is None:
            continue
        else:
            yield clsid
        # if clsid != entityid:
        #     yield from enumerate_subclasses(wdclient, clsid, maxdepth = maxdepth - 1)

def enumerate_instanceofs(entity):
    instancesof = entity.attributes.get('claims',{}).get('P31',[])
    for snak in instancesof:
        id = snak.get("mainsnak",{}).get('datavalue',{}).get("value",{}).get('id',None)
        if id is not None:
            yield id

@cache
def is_organization_subclass(entityid, wdclient):
    @cache
    def is_organization_subclass_recursive(entityid, wdclient, max_depth=10):
        # these are mostly extremely abstract things.
        terminal_nodes = {"Q1379672","Q1003030","Q15401930","Q65938634","Q1756942","Q11348","Q58415929","Q35120","Q23958852","Q151885","Q78754808","Q78754808","Q16686448","Q488383","Q26907166","Q11028","Q4406616","Q30060700","Q2995644","Q733541","Q99527517","Q6671777","Q58778","Q1190554","Q16722960","Q29651519","Q1454986","Q2897903","Q28813620"}

        if entityid in terminal_nodes:
            return False

        if max_depth == 0:
            print(f"Max depth reached for entityid:{entityid}")
            return False

        if entityid is None:
            return False
        subclasses = set(filter(lambda x: (x is not None) or (x != entityid), enumerate_subclasses(wdclient, entityid)))

        if 'Q43229' == entityid:
            return True
        elif 'Q43229' in subclasses:
            return True
        elif any(list(map(partial(is_organization_subclass_recursive,wdclient=wdclient,max_depth=max_depth-1), subclasses))):
            return True
        else:
            return False
    return is_organization_subclass_recursive(entityid, wdclient, max_depth=13)

@cache
def is_wikidata_organization(session, wdclient, name):
    pageprops = session.get(params={'action':'query', 'prop':'pageprops', 'titles':{name},'redirects':True})
    pages = pageprops.get('query',{}).get('pages',{})
    # try resolving redirects
    for _, props in pages.items():
        wikibase_item = props.get('pageprops',{}).get('wikibase_item', None)
        if wikibase_item is not None:
            entity = wdclient.get(wikibase_item)
            instanceofs = enumerate_instanceofs(entity)
            instanceofs = filter(lambda x: x is not None, instanceofs)
            for io in instanceofs:
                if is_organization_subclass(io, wdclient):
                    return True

    return False

links = links_from_article(session,title)

def checklink(link, session, wdclient):
    link['is_organization'] = is_wikidata_organization(session, wdclient, link['wikilink'])
    return link

_checklink = partial(checklink, session=session, wdclient=wdclient)

print(is_organization_subclass("Q15285626",wdclient))
print(is_wikidata_organization(session,wdclient,"Exxon Mobil"))
print("Checking links")
orglinks = list(map(_checklink, links))
orglinks = pd.DataFrame(orglinks)
orglinks.to_csv("../data/sustainable_energy_wikilinks.csv")
