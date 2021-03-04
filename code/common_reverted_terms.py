from mw import api
import deltas
import pickle
import pathlib
import mwreverts
from itertools import chain, islice
from collections import Counter
from multiprocessing import Pool

overwrite = False

revcache_path = pathlib.Path("climate_change_revs.pickle")

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

checksum_revisions = [(rev.get('sha1',None),rev) for rev in all_revs]

reverts = mwreverts.detect(checksum_revisions,radius=15)

all_deleted_tokens = []

def find_reverted_tokens(revert):
    reverteds = revert.reverteds
    initial_revision = reverteds[-1]
    reverted_to = revert.reverted_to
    
    initial_text = initial_revision.get('*',None)
    initial_tokens = deltas.tokenizers.wikitext_split.tokenize(initial_text)
    to_tokens = deltas.tokenizers.wikitext_split.tokenize(reverted_to.get('*',None))

    diff_ops = deltas.segment_matcher.diff(initial_tokens,to_tokens)

    deleted_tokens = (t for t in chain(* [initial_tokens[op.a1:op.a2] for op in diff_ops if op.name=='delete']) if t.type =='word')
    yield from deleted_tokens

def find_reverted_tokens_list(revert):
    return list(find_reverted_tokens(revert))


pool = Pool(3)

all_deleted_tokens = list(pool.map(find_reverted_tokens_list, list(reverts)))

all_deleted_tokens_ = Counter((t.lower() for t in chain(*all_deleted_tokens)))

all_deleted_tokens_

from wordcloud import WordCloud, STOPWORDS

    for op in diff_ops:
        if op.name == 'delete':


        print(op.name, repr(''.join(
            


    text = rev['*']

    if prev is not None:

    prev = rev
    prev_tokens = tokens
    

    diffs.append(api_session.
diffs = api_session.get({'query':'compare','fromrev':first_rev,'torev':last_rev})
diffs = []
prev = None
prev_tokens = None
