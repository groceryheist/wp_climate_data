import pygraphviz as pgv
from pathlib import Path
from collections.abc import Iterable
import pandas as pd
import gzip
import csv

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


def build_network(lines, year):
    lines = lines_from_gzip(f"climate-3hop.wikilink_graph.{year}-03-01.csv.gz")    
    df = pd.DataFrame(lines,columns=header)
    nodes_df_1 = df.loc[:,['page_id_from','page_title_from','N_hops']].drop_duplicates()
    nodes_df_1 = nodes_df_1.rename({'page_id_from':'page_id','page_title_from':'page_title'},axis='columns')
    nodes_df_1.N_hops = nodes_df_1.N_hops.astype(int)
    nodes_df_2 = df.loc[:,['page_id_to','page_title_to','N_hops']].drop_duplicates()
    nodes_df_2 = nodes_df_1.rename({'page_id_to':'page_id','page_title_to':'page_title'},axis='columns')
    nodes_df_2['N_hops'] = nodes_df_2.N_hops.astype(int)
    root_nodes = pd.DataFrame([['6266','Climate change',0],['5042951','Global warming',0]],columns=['page_id','page_title','N_hops'])
    nodes_df = pd.concat([nodes_df_1,nodes_df_2,root_nodes]).drop_duplicates()
    nodes_df = nodes_df.groupby(['page_id','page_title']).min('N_hops').reset_index()

    nodes_df = nodes_df.loc[nodes_df.N_hops < 3]
    
    df2 = df.loc[(df.page_id_to.isin(set(list(nodes_df.page_id)))) & (df.page_id_from.isin(set(list(nodes_df.page_id)))),:]
    df2 = df2.loc[df.page_id_from != df.page_id_to]

    # remove isolates
    nodes_df = nodes_df.loc[nodes_df.page_id.isin(df2.page_id_from) | nodes_df.page_id.isin(df2.page_id_to),:]

    # remove nodes that only have distance 2
    node_pallete = ['#43D548b3', '#43D591B3', '#43D0D5B3','#4387D5B3']
    edge_pallete = ['#6cac6e60', '#6cac8e60', '#6caaac60','#6c8aac60']
#    styles = ["bold","solid","dashed",""]
    g = pgv.AGraph(strict=False,directed=True)
    _ = list(map(lambda n, l, c: g.add_node(n, label='', color=node_pallete[c],fillcolor=node_pallete[c]), list(nodes_df.page_id),list(nodes_df.page_title),list(nodes_df.N_hops)))

    _ = list(map(lambda f,t,s: g.add_edge(f,t,color=edge_pallete[s]), list(df2.page_id_from),
                                  list(df2.page_id_to),
                                  [int(i)
                                   for i in list(df2.N_hops)]))

    g.node_attr['shape'] = 'circle'
    g.node_attr['style'] = 'filled'
    g.edge_attr['arrowhead'] = 'onormal'
    #    g.graph_attr['splines'] = 'true'
    g.graph_attr['overlap'] = 'false'
    g.graph_attr['bgcolor'] = '#8B8D8Bf'
    
    g.layout("sfdp")
    #g.draw(f"graphviz_test_{year}.svg")
    g.draw(f"graphviz_test_{year}.png")

    nodes_df = nodes_df.loc[nodes_df.N_hops < 2]
    
    df2 = df.loc[(df.page_id_to.isin(set(list(nodes_df.page_id)))) & (df.page_id_from.isin(set(list(nodes_df.page_id)))),:]
    df2 = df2.loc[df.page_id_from != df.page_id_to]

    # remove isolates
    nodes_df = nodes_df.loc[nodes_df.page_id.isin(df2.page_id_from) | nodes_df.page_id.isin(df2.page_id_to),:]

    g = pgv.AGraph(strict=False,directed=True)

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
    
    g.write("test.dot")
    g.layout("neato")
    #g.draw(f"graphviz_test_{year}.svg")
    g.draw(f"graphviz_test_{year}_smaller.png")




