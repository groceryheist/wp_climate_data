import pygraphviz as pgv
from pathlib import Path
from collections.abc import Iterable
import pandas as pd
import gzip
import csv
import dask.dataframe as dask
header = ["page_id_from","page_title_from","page_id_to","page_title_to","N_hops"]

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


def build_network(year,  type='full'):
#    lines = lines_from_gzip(f"../data/enwiki.wikilink_graph.{year}-03-01-climate_network.csv.gz")    
    df = dask.read_csv(f"../data/enwiki.wikilink_graph.{year}-03-01-climate_network.csv",dialect='excel-tab',memory_map=True,engine='c',low_memory=True)
    nodes_df_1 = df.loc[:,['page_id_from','page_title_from','N_hops']].drop_duplicates()
    nodes_df_1 = nodes_df_1.rename(columns={'page_id_from':'page_id','page_title_from':'page_title'})
    nodes_df_1.N_hops = nodes_df_1.N_hops.astype(int)
    nodes_df_2 = df.loc[:,['page_id_to','page_title_to','N_hops']].drop_duplicates()
    nodes_df_2 = nodes_df_1.rename(columns={'page_id_to':'page_id','page_title_to':'page_title'})
    nodes_df_2['N_hops'] = nodes_df_2.N_hops.astype(int)
    root_nodes = pd.DataFrame([[6266,'Climate change',0],[5042951,'Global warming',0]],columns=['page_id','page_title','N_hops'])
    nodes_df = dask.concat([nodes_df_1,nodes_df_2,root_nodes]).drop_duplicates()
    nodes_df = nodes_df.groupby(['page_id','page_title']).N_hops.min().reset_index()

    if type == 'full':
        nodes_df = nodes_df.loc[nodes_df.N_hops < 3]

        df2 = df.loc[(df.page_id_to.isin(set(list(nodes_df.page_id)))) & (df.page_id_from.isin(set(list(nodes_df.page_id)))),:]
        df2 = df2.loc[df.page_id_from != df.page_id_to]

        # remove isolates
        nodes_df = nodes_df.loc[nodes_df.page_id.isin(set(list(df2.page_id_from))) | nodes_df.page_id.isin(set(list(df2.page_id_to))),:]

        # remove nodes that only have distance 2
        node_pallete = ['#43D548b3', '#43D591B3', '#43D0D5B3','#4387D5B3','#4387D580']
        edge_pallete = ['#6cac6e60', '#6cac8e60', '#6caaac60','#6c8aac60','#6c8aac30']
    #    styles = ["bold","solid","dashed",""]
        g = pgv.AGraph(strict=False,directed=True)
        print(f'building full network for {year}')
        for row in nodes_df.itertuples():
            n = row.page_id
            l = row.page_title
            c = row.N_hops
            g.add_node(n, label='', color=node_pallete[c],fillcolor=node_pallete[c])
        del(nodes_df)       
        for row in df2.itertuples():
            f = row.page_id_from
            t = row.page_id_to
            s = row.N_hops
            g.add_edge(f,t,color=edge_pallete[s])
        del(df2)
        g.node_attr['shape'] = 'circle'
        g.node_attr['style'] = 'filled'
        g.edge_attr['arrowhead'] = 'onormal'
        #    g.graph_attr['splines'] = 'true'
        g.graph_attr['overlap'] = 'false'
        g.graph_attr['bgcolor'] = '#8B8D8Bf'

        print('computing layout')

        g.layout("sfdp")
        #g.draw(f"graphviz_test_{year}.svg")

        print('rendering')
        g.draw(f"graphviz_test_{year}.png")
        print('done')

    if type == 'smaller':
        nodes_df = nodes_df.loc[nodes_df.N_hops < 2]
    
        df2 = df.loc[(df.page_id_to.isin(set(list(nodes_df.page_id)))) & (df.page_id_from.isin(set(list(nodes_df.page_id)))),:]
        df2 = df2.loc[df.page_id_from != df.page_id_to]

        # remove isolates
        nodes_df = nodes_df.loc[nodes_df.page_id.isin(df2.page_id_from) | nodes_df.page_id.isin(df2.page_id_to),:]

        g = pgv.AGraph(strict=False,directed=True)

        print(fe"building smaller network for {year}")

        _ = list(map(lambda n, l, c: g.add_node(n, label=l, fillcolor=node_pallete[c], color=node_pallete[c]), list(nodes_df.page_id),list(nodes_df.page_title),list(nodes_df.N_hops)))

        _ = list(map(lambda f,t,s: g.add_edge(f,t,style=s),
                     list(df2.page_id_from),
                     list(df2.page_id_to),
                     [styles[int(i)]
                      for i in list(df2.N_hops)]))

        g.node_attr['shape'] = 'plaintext'
        g.node_attr['style'] = 'filled'
        g.edge_attr['arrowhead'] = 'onormal'
        g.graph_attr['splines'] = 'true'
        g.graph_attr['overlap'] = 'false'
        g.graph_attr['bgcolor'] = '#8B8D8Bf'

        print("computing layout")
        g.layout("neato")
        #g.draw(f"graphviz_test_{year}.svg")
        print('rendering')
        g.draw(f"graphviz_test_{year}_small.png")

build_network("2018",'full')
build_network("2018",'smaller')
