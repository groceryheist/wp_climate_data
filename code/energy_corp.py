from mw import api
from datetime import datetime 
from mw.types.timestamp import Timestamp
from itertools import chain, islice
from functools import partial
import re
import pandas as pd
from dataclasses import dataclass
from multiprocessing import Pool

apiurl = "https://en.wikipedia.org/w/api.php"
session = api.Session(apiurl)

# instead of using the lists I'm going to use wikidata.
# reason being that recursively navigating the categories basically included everything.
# i think that wikidata should be more precise.

companies = pd.read_csv("../data/energy_companies.csv")


# links_to_lists = ["https://en.wikipedia.org/wiki/Category:Automotive_fuel_retailers",
#                   "https://en.wikipedia.org/wiki/List_of_oil_exploration_and_production_companies",
#                   "https://en.wikipedia.org/wiki/List_of_United_States_electric_companies",
#                   "https://en.wikipedia.org/wiki/List_of_Public_Utilities",
#                   "https://en.wikipedia.org/wiki/Category:Renewable_energy_companies",
#                   "https://en.wikipedia.org/wiki/Category:Lists_of_energy_companies",
#                   "https://en.wikipedia.org/wiki/Category:Geotechnical_engineering_companies",
#                   "https://en.wikipedia.org/wiki/List_of_environmental_organizations",
#                   "https://en.wikipedia.org/wiki/Category:Carbon_capture_and_sequestration"]

# categories = ['Category:Automotive_fuel_retailers','Category:Renewable_energy_companies','Category:Lists_of_energy_companies','Category:Geotechnical_engineering_companies','Category:Carbon_capture_and_sequestration','Category:Oil and gas companies','Category:Energy companies']

def pages_from_category(session, category):
    cm = session.get(params={'action':'query','list':"categorymembers",'cmtitle':category})
    for res in cm['query']['categorymembers']:
        yield res

    cont = cm.get('continue', None)

    while cont is not None:
#        print(cont)
        cm = session.get(params={'action':'query','list':"categorymembers",'cmtitle':category,'cmcontinue':cont['cmcontinue']})
        for res in cm['query']['categorymembers']:
            yield res
        cont = cm.get('continue', None)


def pages_from_category_recursive(session, category, checked_categories = None):
    print(category)
    categories = {category}
    if checked_categories is not None:
        categories = categories.union(checked_categories)
    pages = pages_from_category(session, category)
    for page in pages:
        if page['ns'] == 0:
            yield page['title']
        if page['ns'] == 1:
            yield page['title'].replace("Talk:","")
        if page['ns'] == 14 and page['title'] not in categories:
            if page['title'] != 'Wikipedia_categories_named_after_energy_companies':
                categories.add(page['title'])
                yield from pages_from_category_recursive(session, page['title'], categories)


# pages_from_categories = list(chain(* map(lambda cat: pages_from_category_recursive(session, cat), categories)))

# ns0_pages = set(page['title'] for page in list(filter(lambda page: page['ns']==0, pages_from_categories)))

climate_change_pages = list(pages_from_category_recursive(session, "Category:Climate_change_articles_by_quality"))

# build a massive regular expression to find matches
company_names = set(companies.loc[~companies.companydesc.isna(),"companydesc"])
big_regex = re.compile('(' + ( "|".join(f"[\s\[\[]{name}[\s\]\]]" for name in company_names) ).replace('_','[\s_]') + ')',flags=re.I)

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

def find_mentions(rev, big_regex):
        yield from big_regex.finditer(rev.get('*',""))
    

@dataclass
class CompanyMention:
    company:str
    timestamp:datetime
    page:str
    revid:int

i = 0 
company_mentions = []
page =  "petroleum industry"

def process_rev(rev, page):
    timestamp = datetime.fromtimestamp(Timestamp(rev['timestamp']).serialize())        
    mentions = find_mentions(rev, big_regex)
    for mention in mentions:
        groups = filter(lambda g: g is not None,mention.groups())
        g = mention[0].strip("[]").strip()
        yield CompanyMention(company=g, timestamp=timestamp, page=page,revid=rev['revid'])

def _process_rev(*args, **kwargs):
    return list(process_rev(*args, **kwargs))

pool = Pool(4)
for page in climate_change_pages:
    print(page)
    revs = get_monthly_revs(session, page)
    company_mentions.extend(chain(*pool.map(partial(_process_rev, page=page),
                                            revs)))

print(company_mentions)
output = pd.DataFrame(company_mentions)
output.to_csv("../data/company_mentions.csv")
