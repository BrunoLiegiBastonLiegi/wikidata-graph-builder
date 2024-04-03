import argparse, requests, time, os, sys

from langchain_community.llms import Ollama
from langchain.prompts import PromptTemplate

from to_graph import load_entities
sys.path.append("./wikidata-disamb")
from prepare import load


SPARQL_ENDPOINT = "https://query.wikidata.org/bigdata/namespace/wdq/sparql" 
QUERY = """PREFIX schema: <http://schema.org/> 
PREFIX wd: <http://www.wikidata.org/entity/> 

SELECT {_vars} WHERE {{
{expr}
}}"""


def dump(entities: list[str], descriptions: list[str], filename: str):
    with open(filename, "w") as f:
        for i, (e, d) in enumerate(zip(entities, descriptions)):
            if d is None:
                d = str(None)
            f.write(f"{e} {d}")
            if i != len(entities):
                f.write("\n")

                
def descriptions_query(entities: list[str]) -> list[str]:
    descriptions = []
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
            _, desc = zip(*sorted(list(data.items()), key=lambda x: int(x[0].replace("d", ""))))
            descriptions += desc
            time.sleep(0.5)
        else:
            print(f"> {status}: error.")
            raise RuntimeError
    return descriptions


PROMPT = PromptTemplate.from_template("A Wikidata entity is provided below. Generate a short one-sentence long description of the entity.\nEntity: {entity}")

def generate_missing_description(entity: str, llm):
    return llm.invoke(PROMPT.format(entity=entity))


def get_descriptions(entities: set[str] | list[str], batchsize: int=20) -> list[str]:
    global entities_dir, descriptions_bkup
    missing_ents = [e for e in entities if e not in descriptions_bkup]
    
    descriptions = list(descriptions_bkup.values())
    ent_ids = list(descriptions_bkup.keys())
    for i in range(0, len(missing_ents), batchsize):
        ents = missing_ents[i:i + batchsize]
        descriptions += descriptions_query(ents)
        ent_labels = missing_ents[:i + batchsize]
        print(f"({i + len(descriptions_bkup)}/{len(entities)})", end="\r")
        if i % 10 == 0:
            dump(list(descriptions_bkup.keys()) + missing_ents[:i + batchsize], descriptions, f"{entities_dir}/descriptions.txt")
    print("\n")
    return ent_ids, descriptions
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--entities")
    parser.add_argument("--generate_missing", action="store_true")
    args = parser.parse_args()

    entities = set(load_entities(args.entities))
    entities_dir = os.path.dirname(args.entities)
    entity_labels = load("./wikidata/names.txt", as_list=False)
    try:
        descriptions_bkup = load(f"{entities_dir}/descriptions.txt", as_list=False)
    except FileNotFoundError:
        descriptions_bkup = {}
    entity_ids, descriptions = get_descriptions(list(entities))
    

    if args.generate_missing:
        llm = Ollama(model="llama2:13b")
        entity_labels = [entity_labels[i] for i in entity_ids]
        for i, d in enumerate(descriptions):
            if d is None or d == "None" or "Wikimedia" in d:
                breakpoint()
                descriptions[i] = generate_missing_description(entity_labels[i], llm)

    outfile = f"{entities_dir}/descriptions"
    if args.generate_missing:
        outfile = f"{outfile}_generated_missing"
    outfile = f"{outfile}.txt"
    with open(outfile, "w") as f:
        for i, (e, d) in enumerate(zip(entities, descriptions)):
            if d is None:
                d = str(None)
            f.write(f"{e} {d}")
            if i != len(entities):
                f.write("\n")
