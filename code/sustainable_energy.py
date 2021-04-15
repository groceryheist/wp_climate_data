from mw import api
from datetime import datetime 
from mw.types.timestamp import Timestamp
from itertools import chain, islice
from functools import partial, lru_cache
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

@lru_cache(maxsize=1028)
def enumerate_subclasses(wdclient, entityid):
    entity = wdclient.get(entityid)
    subclassof = entity.attributes.get('claims',{}).get('P279',[])
    for cls in subclassof:
        clsid = cls.get("mainsnak",{}).get('datavalue',{}).get("value",{}).get('id',None)
        if clsid is not None:
            yield clsid
            yield from enumerate_subclasses(wdclient, clsid)

def enumerate_instanceofs(entity):
    instancesof = entity.attributes.get('claims',{}).get('P31',[])
    for snak in instancesof:
        id = snak.get("mainsnak",{}).get('datavalue',{}).get("value",{}).get('id',None)
        if id is not None:
            yield id
            
@lru_cache(maxsize=10000)
def is_wikidata_organization(session, wdclient, name):
    pageprops = session.get(params={'action':'query', 'prop':'pageprops', 'titles':{name},'redirects':True})
    pages = pageprops.get('query',{}).get('pages',{})
    # try resolving redirects
    for _, props in pages.items():
        wikibase_item = props.get('pageprops',{}).get('wikibase_item', None)
        if wikibase_item is not None:
            entity = wdclient.get(wikibase_item)
            instanceofs = enumerate_instanceofs(entity)
            for io in instanceofs:
                subclasses = enumerate_subclasses(wdclient, io)
                if 'Q43229' in subclasses:
                    return True
    return False

links = links_from_article(session,title)

def checklink(link, session, wdclient):
    link['is_organization'] = is_wikidata_organization(session, wdclient, link['wikilink'])
    return link

_checklink = partial(checklink, session=session, wdclient=wdclient)
orglinks = list(map(_checklink, links))
orglinks = pd.DataFrame(orglinks)
orglinks.to_csv("data/sustainable_energy_wikilinks.csv")
