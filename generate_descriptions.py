import json, os, requests, sys, time

from bs4 import BeautifulSoup
from tqdm import tqdm

from langchain_community.llms import Ollama
from langchain.prompts import PromptTemplate
from multiprocessing import Pool

from to_graph import load_entities

SPARQL_ENDPOINT = "https://query.wikidata.org/bigdata/namespace/wdq/sparql" 

def query_wikipedia_link(var, entity):
    return f"""
    OPTIONAL {{
        {var} schema:about wd:{entity} .
        {var} schema:inLanguage "en" .
        FILTER (SUBSTR(str({var}), 1, 25) = "https://en.wikipedia.org/")
    }}
    """


def get_wikipedia_links_query(entities):
    variables = [f"?link{i}" for i in range(len(entities))]
    expr = "".join([query_wikipedia_link(var, ent) for var, ent in zip(variables, entities)])
    return f"""
    prefix schema: <http://schema.org/>
    PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    
    SELECT {" ".join(variables)} WHERE {{
        {expr}
    }} 
    """


def get_wikipedia_link(entities):
    status = None
    while status != 200:
        r = requests.get(
            SPARQL_ENDPOINT,
            params = {'format': 'json', 'query': get_wikipedia_links_query(entities)}
        )
        status = r.status_code
        if status != 200:
            print(f"-> received {status} response.")
            time.sleep(0.3)
        
        
    r = r.json()
    var = r["head"]["vars"]
    links = []
    for v in var:
        if v in r["results"]["bindings"][0]:
            links.append(r["results"]["bindings"][0][v]["value"])
        else:
            links.append(None)
    return links


def wikipedia_paragraph_extractor(link):
    par = None
    if link is not None:
        status = None
        while status != 200:
            r = requests.get(link)
            status = r.status_code
            if status != 200:
                print(f"-> received {status} response.")
                time.sleep(1)
        soup = BeautifulSoup(r.content, "html.parser")
        ps = soup.find_all("p")
        par = "".join(map(lambda x: x.get_text(), ps[:5]))
    return par


def extract_wikipedia_paragraph(entities):
    links = get_wikipedia_link(entities)
    with Pool(6) as p:
        paragraphs = p.map(wikipedia_paragraph_extractor, links)
    return paragraphs
    


PROMPT = PromptTemplate.from_template("A Wikidata entity is provided below. Generate a short one-sentence long description of the entity.\nEntity: {entity}")

def generate_missing_description(entity: str, llm):
    breakpoint()
    return llm.invoke(PROMPT.format(entity=entity))




if __name__ == "__main__":

    entities = list(set(load_entities(sys.argv[1])))
    working_dir = os.path.dirname(sys.argv[1])
    batchsize = 20
    paragraphs = []
    for i in tqdm(range(0, len(entities), batchsize), total=int(len(entities)/batchsize)):  
        for par in extract_wikipedia_paragraph(entities[i:i + batchsize]):
            #print(f"\n-------------------------------------------------\n{par}\n----------------------------------------------\n")
            paragraphs.append(par)
    ent2wikipeda = dict(zip(entities, paragraphs))
    with open(f"{working_dir}/wikipedia_pages.json", "w") as f:
        json.dump(ent2wikipeda, f, indent=2)
    
    llm = Ollama(model="llama2:13b")
    entity_labels = [id_to_label[i] for i in entity_ids]
    for i, d in enumerate(descriptions):
        if d is None or d == "None" or "Wikimedia" in d:
            print(entity_ids[i])
            descriptions[i] = generate_missing_description(entity_labels[i], llm)
