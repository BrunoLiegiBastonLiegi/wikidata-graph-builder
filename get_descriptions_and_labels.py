import argparse, requests, time, os, sys

from to_graph import load_entities
sys.path.append("./wikidata-disamb")
from prepare import load


SPARQL_ENDPOINT = "https://query.wikidata.org/bigdata/namespace/wdq/sparql" 
QUERY = """PREFIX schema: <http://schema.org/> 
PREFIX wd: <http://www.wikidata.org/entity/> 

SELECT {_vars} WHERE {{
{expr}
}}"""


def dump(filename: str, ids: list[str], data: list[str] = None):
    if data is not None and len(ids) != len(data):
        raise RuntimeError(f"ids and data have different lenghts: {len(ids)} and {len(data)}")
    with open(filename, "w") as f:
        items = zip(ids, data) if data is not None else ids
        for i, item in enumerate(items):
            if data is not None:
                f.write(f"{item[0]} {str(item[1])}")
            else:
                f.write(f"{item}")
            if i != len(ids):
                f.write("\n")


def redirections_query(entities: list[str]):
    _vars = [f"?r{i}" for i in range(len(entities))]
    expr = "\n".join([
        f"OPTIONAL {{ wd:{e} owl:sameAs {_vars[i]}. }}"
        for i, e in enumerate(entities)
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
            data = {f"r{i}": None for i in range(len(entities))}
            for b in bindings:
                for d, val in b.items():
                    if data[d] is None:
                        data[d] = val["value"]
            _, redirections = zip(*sorted(list(data.items()), key=lambda x: int(x[0].replace("r", ""))))
            redirections = list(redirections)
            for i in range(len(redirections)):
                if redirections[i] is not None:
                    redirections[i] = redirections[i].replace("http://www.wikidata.org/entity/", "")
            redirections = tuple(redirections)
            time.sleep(0.5)
        else:
            print(f"> {status}: error.")
            raise RuntimeError
    return redirections

                
def descriptions_query(entities: list[str], check_for_redirections: bool=True) -> list[str]:
    _vars = [f"?d{i}" for i in range(len(entities))]
    expr = "\n".join([
        f"OPTIONAL {{ wd:{e} schema:description {_vars[i]}.\nFILTER (langMatches( lang({_vars[i]}), \"EN\" )). }}"
        for i, e in enumerate(entities)
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
            data = {f"d{i}": None for i in range(len(entities))}
            for b in bindings:
                for d, val in b.items():
                    if data[d] is None:
                        data[d] = val["value"]
            _, descriptions = zip(*sorted(list(data.items()), key=lambda x: int(x[0].replace("d", ""))))
            time.sleep(0.5)
        else:
            print(f"> {status}: error.")
            raise RuntimeError
    none_idx = [i for i,d in enumerate(descriptions) if d is None or "Wikimedia" in d]
    if check_for_redirections and len(none_idx) > 0:
        breakpoint()
        redirected_ents = redirections_query([entities[i] for i in none_idx])
        redirected_desc = descriptions_query(redirected_ents, check_for_redirections=False)
        descriptions = list(descriptions)
        for i, desc in zip(none_idx, redirected_desc):
            descriptions[i] = desc
        descriptions = tuple(descriptions)
    return descriptions


def labels_query(entities: list[str], check_for_redirections: bool=True) -> list[str]:
    _vars = [f"?l{i}" for i in range(len(entities))]
    expr = "\n".join([
        f"OPTIONAL {{ wd:{e} rdfs:label {_vars[i]}.\nFILTER (langMatches( lang({_vars[i]}), \"EN\" )). }}"
        for i, e in enumerate(entities)
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
            data = {f"l{i}": None for i in range(len(entities))}
            for b in bindings:
                for l, val in b.items():
                    if data[l] is None:
                        data[l] = val["value"]
            _, labels = zip(*sorted(list(data.items()), key=lambda x: int(x[0].replace("l", ""))))
            time.sleep(1)
        else:
            print(f"> {status}: error.")
            raise RuntimeError
    none_idx = [i for i,l in enumerate(labels) if l is None]
    if check_for_redirections and len(none_idx) > 0:
        redirected_ents = redirections_query([entities[i] for i in none_idx])
        redirected_labels = labels_query(redirected_ents, check_for_redirections=False)
        labels = list(labels)
        for i, lab in zip(none_idx, redirected_labels):
            labels[i] = lab
        labels = tuple(labels)
    return labels


def get_redirections(entities: set[str] | list[str], batchsize: int=20) -> list[str]:
    print("> Looking for redirected entities")
    global entities_dir, redirections_bkup
    missing_ents = [e for e in entities if e not in redirections_bkup]

    redirections = list(redirections_bkup.values())
    ent_ids = list(redirections_bkup.keys())
    for i in range(0, len(missing_ents), batchsize):
        ents = missing_ents[i:i + batchsize]
        redirections += redirections_query(ents)
        ent_ids += ents
        ent_labels = missing_ents[:i + batchsize]
        print(f"({i + len(redirections_bkup)}/{len(entities)})", end="\r")
        if i % (10 * batchsize) == 0:
            dump(f"{entities_dir}/../redirections.txt", list(redirections_bkup.keys()) + missing_ents[:i + batchsize], redirections)
    print("\n")
    return ent_ids, redirections


def get_descriptions(entities: set[str] | list[str], batchsize: int=20) -> list[str]:
    print("> Collecting entity descriptions")
    global entities_dir, descriptions_bkup
    missing_ents = [e for e in entities if e not in descriptions_bkup]
    
    descriptions = list(descriptions_bkup.values())
    ent_ids = list(descriptions_bkup.keys())
    for i in range(0, len(missing_ents), batchsize):
        ents = missing_ents[i:i + batchsize]
        descriptions += descriptions_query(ents)
        ent_ids += ents
        ent_labels = missing_ents[:i + batchsize]
        print(f"({i + len(descriptions_bkup)}/{len(entities)})", end="\r")
        if i % (10 * batchsize) == 0:
            dump(f"{entities_dir}/descriptions.txt", list(descriptions_bkup.keys()) + missing_ents[:i + batchsize], descriptions)
    print("\n")
    return ent_ids, descriptions


def get_labels(entities: set[str] | list[str], batchsize: int=20) -> list[str]:
    print("> Collecting entity labels")
    global entities_dir, labels_bkup
    missing_ents = [e for e in entities if e not in labels_bkup]
    
    labels = list(labels_bkup.values())
    ent_ids = list(labels_bkup.keys())
    for i in range(0, len(missing_ents), batchsize):
        ents = missing_ents[i:i + batchsize]
        labels += labels_query(ents)
        ent_ids += ents
        ent_ids = missing_ents[:i + batchsize]
        print(f"({i + len(labels_bkup)}/{len(entities)})", end="\r")
        if i % (10 * batchsize) == 0:
            dump(f"{entities_dir}/labels.txt", list(labels_bkup.keys()) + missing_ents[:i + batchsize], labels)
    print("\n")
    return ent_ids, labels
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--entities")
    args = parser.parse_args()

    entities = set(load_entities(args.entities))
    entities_dir = os.path.dirname(args.entities)

    def load_backup(filename, as_list=False):
        try:
            return load(filename, as_list=as_list)
        except FileNotFoundError:
            return {}

    redirections_bkup = load_backup(f"{entities_dir}/../redirections.txt")
    labels_bkup = load_backup(f"{entities_dir}/labels.txt")
    descriptions_bkup = load_backup(f"{entities_dir}/descriptions.txt")
    breakpoint()
    entity_ids, redirections = get_redirections(list(entities))
    outfile = f"{entities_dir}/../redirections.txt"
    dump(outfile, entity_ids, redirections)
    
    entity_ids, labels = get_labels(list(entities))
    outfile = f"{entities_dir}/labels.txt"
    dump(outfile, entity_ids, labels)

    entity_ids, descriptions = get_descriptions(list(entities))
    outfile = f"{entities_dir}/descriptions.txt"
    dump(outfile, entity_ids, descriptions)
