import requests, time, argparse

from itertools import permutations

def load_entities(path: str) -> list[str]:
    with open(path, "r") as f:
        ids = f.readlines()
    return ids

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

def find_relations(heads: list[str], tails: list[str]) -> list[str]:
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
            time.sleep(0.3)
            return relations
        else:
            print(f"> {status}: error.")
            raise RuntimeError


def construct_graph_from_entities(entities: list[str], batchsize: int=100) -> list[tuple]:
    head_tail_pairs = list(permutations(entities, 2))
    triplets = []
    for i in range(0, len(head_tail_pairs), batchsize):
        heads, tails = list(zip(*head_tail_pairs[i:i + batchsize]))
        relations = find_relations(heads, tails)
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
    parser.add_argument("--outfile")
    args = parser.parse_args()
    
    entities = load_entities(ars.entities)
    triplets = construct_graph_from_entities(entities)
    dump_graph(triplets, args.outfile)
