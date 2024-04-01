import argparse, requests, time

from to_graph import load_entities

SPARQL_ENDPOINT = "https://query.wikidata.org/bigdata/namespace/wdq/sparql" 
QUERY = """PREFIX schema: <http://schema.org/> 
PREFIX wd: <http://www.wikidata.org/entity/> 

SELECT {_vars} WHERE {{
{expr}
}}"""

def get_descriptions(entities: set[str] | list[str], batchsize: int=20) -> list[str]:
    _vars = [f"?d{i}" for i in range(batchsize)]
    descriptions = []
    for i in range(0, len(entities), batchsize):
        ents = entities[i:i + batchsize]
        expr = "\n".join([
            f"OPTIONAL {{ wd:{e} schema:description {_vars[i]}.\nFILTER (langMatches( lang({_vars[i]}), \"EN\" )). }}"
            for i, e in enumerate(ents)
        ])
        query = QUERY.format(_vars=" ".join(_vars), expr=expr)
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
                data = {f"d{i}": None for i in range(batchsize)}
                for b in bindings:
                    for d, val in b.items():
                        if data[d] is None:
                            data[d] = val["value"]
                _, desc = zip(*sorted(list(data.items()), key=lambda x: int(x[0].replace("d", ""))))
                descriptions += desc
                time.sleep(0.5)
            else:
                print(f"> {status}: error.")
                raise RuntimeError
        print(f"({i}/{len(entities)})", end="\r")
    print("\n")
    return descriptions
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--entities")
    parser.add_argument("--outfile")
    args = parser.parse_args()
    
    entities = set(load_entities(args.entities))
    descriptions = get_descriptions(list(entities))
    with open(args.outfile, "w") as f:
        for i, (e, d) in enumerate(zip(entities, descriptions)):
            if d is None:
                d = str(None)
            f.write(f"{e} {d}")
            if i != len(entities):
                f.write("\n")
