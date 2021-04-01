import numpy as np
import plotnine as pn
import pandas as pd
from mw import api
from mw.types.timestamp import Timestamp
from datetime import datetime
import deltas
import pickle
from pathlib import Path
import mwreverts
from itertools import chain, islice
from collections import Counter
from wordcloud import WordCloud, STOPWORDS
from multiprocessing import Pool
from mwtext import Wikitext2Words
from zipfile import ZipFile

overwrite = False

revcache_path = Path("data/climate_change_revs.pickle")

def update_revs():
    api_session = api.Session("https://en.wikipedia.org/w/api.php")

    rv_props =  {'revid' : 'ids',
                 'timestamp' : 'timestamp',
                 'user' : 'user',
                 'userid' : 'userid',
                 'size' : 'size',
                 'sha1' : 'sha1',
                 'contentmodel' : 'contentmodel',
                 'tags' : 'tags',
                 'flags' : 'flags',
                 'comment' : 'comment',
                 'content' : 'content' }

    revs = api_session.revisions.query(properties=rv_props.values(),
                                       titles={'climate change'},
                                       direction="newer")

    all_revs = list(revs)
    pickle.dump(all_revs,open(revcache_path,'wb'))
    return all_revs

if revcache_path.exists() and overwrite is False:
    all_revs = pickle.load(open(revcache_path,'rb'))
else:
    all_revs = update_revs()

denial_words = ['alarm','panic','fabricated','falsified','false','inaccurate','distorted','holocene','epoch','interglacial','sediments','medieval','ancient','quaternary','panic','dubious','fraud','unproven','flawed','myth','notions','push','agenda','plant','produce','economic','growth','claim','assertion','skepticism','cycle','doubts','concern','hypothesizes','speculates','speculate','postulate','controversy']

# monthly snapshot of revisions
last_month = None
monthly_revs = []
monthly_words = {}
all_words = set(denial_words)

def transform_siteInfo(siteInfo):
    new_siteInfo = {}
    for k, v in siteInfo.items():
        if k=='namespaces':
            if k not in new_siteInfo.keys():
                new_siteInfo[k] = {}
            for k2, v2 in v.items():
                new_dict = v2.copy()
                for k3, v3 in v2.items():
                    if k3 == '*':
                        new_dict['name']=v3
                new_siteInfo[k][k2]=new_dict
        if k=='namespacealiases':
            new_siteInfo[k] = []
            for nsalias in v:
                new_alias = nsalias.copy()
                for k2, v2 in nsalias.items():
                    if k2 == '*':
                        new_alias['alias'] = v2
                new_siteInfo[k].append(new_alias)

    return new_siteInfo

session = api.Session("https://en.wikipedia.org/w/api.php")
siteInfo = session.site_info.query(properties={'namespaces','namespacealiases'})
new_siteInfo = transform_siteInfo(siteInfo)
text2words = Wikitext2Words.from_siteinfo(new_siteInfo)

def add_month(rev, month):
    global all_words
    global monthly_revs
    global montly_words
    monthly_revs.append(rev)
    wikitext = text2words.transform(rev["*"])
    words = list(wikitext)
    all_words = all_words.union(set(words))
    monthly_revs.append(rev)
    monthly_words[month] = words


for rev in all_revs:
    timestamp = datetime.fromtimestamp(Timestamp(rev['timestamp']).serialize())
    month =  timestamp.replace(day=1,hour=1,minute=1,second=1)
    if last_month is None:
        last_month = month
        add_month(rev, month)
    elif last_month < month:
        last_month = month
        add_month(rev,month)
    else:
        continue

del(all_revs)

def load_glove(wordlist):
    glove_path = Path("data/glove.6B.zip")
    zipfile = ZipFile(glove_path)
    names = zipfile.namelist()
    lines = zipfile.open("glove.6B.300d.txt")
    glove_dict = {}
    for line in lines:
        parts = line.decode().strip().split(' ')
        token = parts[0]
        if token in wordlist:
            vector = np.array([float(c) for c in parts[1:]])
            glove_dict[token] = vector

    return glove_dict

glove_dict = load_glove(all_words)

def nearest_neighbors(word, glove_dict,N=10):
    wordvec = glove_dict[word]
    sims = ( (term, cossim(wordvec,othervec)) for term, othervec in glove_dict.items())
    return sorted(sims, key = lambda x: -1*x[1])[:10]
    
def doc2vec(words, glove_dict):
    N = len(words)
    vec = None
    for word in words:
        res = glove_dict.get(word, None)
        if res is not None:
            if vec is None:
                vec = res
            else:
                vec = vec + res
    return vec / N

denial_vec = doc2vec(denial_words, glove_dict)

monthly_vecs = {month: doc2vec(words, glove_dict) for month, words in monthly_words.items()}

def cossim(vec1, vec2):
    norm = np.linalg.norm(vec1) * np.linalg.norm(vec2)
    sim = np.dot(vec1,vec2) / norm
    return sim

monthly_denial_sims = [{"month":month,"similarity":cossim(denial_vec,vec)} for month, vec in monthly_vecs.items()]
df = pd.DataFrame(monthly_denial_sims)

p = pn.ggplot(df, pn.aes(x="month",y="Similarity with denialism words")) + pn.geom_line() 
p.save("Climate_denialism_trend.png")
