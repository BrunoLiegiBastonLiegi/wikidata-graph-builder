import requests, time, argparse
import networkx as nx
import matplotlib.pyplot as plt

from itertools import permutations


def load_entities(path: str) -> list[str]:
    with open(path, "r") as f:
        ids = f.read().splitlines()
    return ids


def load_rdf_triplets(path: str) -> list[tuple]:
    with open(path, "r") as f:
        triplets = f.readlines()
    return [tuple(t.replace("\n", "").split(" ")) for t in triplets]


QUERY = """PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT {r_vars}
WHERE
{{
  {triplets}
}}
"""

SPARQL_ENDPOINT = "https://query.wikidata.org/bigdata/namespace/wdq/sparql" 


def query_for_relations(heads: list[str], tails: list[str]) -> list[str]:
    if len(heads) != len(tails):
        raise RuntimeError
    triplets = "\n  ".join([
        f"OPTIONAL {{ wd:{h} ?r{i} wd:{t}. }}"
        for i, (h,t) in enumerate(zip(heads, tails))
    ])
    r_vars = " ".join([f"?r{i}" for i in range(len(heads))])
    query = QUERY.format(r_vars=r_vars, triplets=triplets)
    status = None
    while status != 200:
        r = requests.get(
            SPARQL_ENDPOINT,
            params = {'format': 'json', 'query': query}
        )
        status = r.status_code
        if status == 429:
            print("> 429: too many requests.")
            time.sleep(3)
        elif status == 200:
            bindings = r.json()["results"]["bindings"]
            relations = []
            if len(bindings) > 0:
                bindings = bindings[0]
                for i in range(len(heads)):
                    if f"r{i}" in bindings:
                        relations.append(bindings[f"r{i}"]["value"])
                    else:
                        relations.append(None)
            else:
                relations = [None for _ in range(len(heads))]    
            time.sleep(0.5)
            return relations
        else:
            print(f"> {status}: error.")
            raise RuntimeError


def construct_graph_from_entities(entities: set[str], query=False, batchsize: int=100) -> list[tuple]:
    triplets = []
    if not query:
        global rdf_triplets
        for head, rel, tail in rdf_triplets:
            if head in entities and tail in entities:
                triplets.append((head, rel, tail))
        return triplets
            
    head_tail_pairs = list(permutations(entities, 2))
    for i in range(0, len(head_tail_pairs), batchsize):
        heads, tails = list(zip(*head_tail_pairs[i:i + batchsize]))
        relations = query_for_relations(heads, tails)
        for h, r, t in zip(heads, relations, tails):
            if r is not None:
                triplets.append((h, r, t))
        print(f"> Searching for relations in Wikidata. ({i}/{len(head_tail_pairs)})", end="\r")
    print("\n")
    return triplets


def dump_graph(triplets: list, path: str):
    with open(path, "w") as f:
        for t in triplets:
            f.write(" ".join(t) + "\n")

            
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--entities")
    parser.add_argument("--outfile", default="graph.txt")
    parser.add_argument("--rdf")
    parser.add_argument("--visualize", action="store_true")
    args = parser.parse_args()
    
    entities = set(load_entities(args.entities))
    if args.rdf is not None:
        rdf_triplets = load_rdf_triplets(args.rdf)
        do_query = False
    else:
        rdf_triplets = None
        do_query = True
    triplets = construct_graph_from_entities(entities, query=do_query)
    if args.visualize:
        # visualize and analyze the graph
        graph = nx.DiGraph()
        graph.add_nodes_from(entities)
        for head, rel, tail in triplets:
            graph.add_edge(head, tail, type=rel)
        print(f"""
        -------------- Graph --------------
        
        > Number of Nodes: {len(graph.nodes)}
        > Number of Edges: {len(graph.edges)}
        > Number of connected Nodes: {nx.number_connected_components(graph.to_undirected())}
        
        -----------------------------------
        """)
        fig, ax = plt.subplots(figsize=(9, 9), facecolor='lightskyblue', layout='constrained')
        nx.draw(graph, ax=ax)
        plt.savefig("graph.pdf", format="pdf", dpi=300)
        
    dump_graph(triplets, args.outfile)
