import argparse, json, re, sys
sys.path.append("../")

from get_descriptions_and_labels import load_entities
from to_graph import dump_graph

from pathlib import Path
from sklearn.model_selection import train_test_split


def load(filename, as_list=True):
    _dict = {}
    with open(filename, "r") as f:
        for line in f.read().split("\n"):
            if len(line) <= 1:
                continue
            _match = re.search("[QP][0-9]+ ", line)
            _id = line[:_match.span()[1] - 1]
            data = line[_match.span()[1]:]
            if as_list:
                if _id in _dict:
                    _dict[_id].append(data)
                else:
                    _dict[_id] = [data]
            else:
                _dict[_id] = data
    return _dict


def load_graph(filename):
    edges = []
    with open(filename, "r") as f:
        for line in f.read().split("\n"):
            if len(line) <= 1:
                continue
            head, rel, tail = line.split(" ")
            edges.append((head, rel, tail))
    return edges

def prepare_pretraining_data(ents):
    global descriptions, names, entities
    
    pretraining_data = {}
    for _id in ents:
        if _id not in entities:
            continue
        caption = descriptions.get(_id, None)
        label = names.get(_id, None)
        if caption is not None:
            caption = caption[0]
        if label is not None:
            label = label[0]
        pretraining_data[_id] = {
            "wikidata_id": _id,
            "caption": caption,
            "label": label,
            "entity_id": _id,
        }
    return pretraining_data


def extract_entities_from_disamb_data(data):
    ents = set()
    for d in data:
        if "alternative_id" in d:
            ents.add(d["alternative_id"])
        else:
            ents.add(d["correct_id"])
        ents.add(d["wrong_id"])
    return ents


def cut_dataset(dataset, graph, entities, base_dir, el_data):
    edges = dict(zip(graph.copy(), range(len(graph))))
    for _set in ("train", "dev", "test"):
        elements_to_delete = []
        for _id, metadata in dataset[_set].items():
            if metadata["caption"] == "None" or "Wikimedia" in metadata["caption"]:
                elements_to_delete.append(_id)
        edges_to_delete = []
        for el in elements_to_delete:
            dataset[_set].pop(el)
            entities.pop(el, None)
            for edge in edges:
                if el in edge:
                    edges_to_delete.append(edge)
        for edge in edges_to_delete:
            edges.pop(edge, None)
                    
        with open(f"{base_dir}-cut/pretraining/{_set}.json", "w") as f:
            json.dump(dataset[_set], f, indent=2)

    edges = list(edges)
    dump_graph(edges, f"{base_dir}-cut/link-prediction/train.txt")

    entities = dict(zip(entities, range(len(entities))))
    relations = {triplet[1] for triplet in edges}
    relations = dict(zip(relations, range(len(relations))))
    with open(f"{base_dir}-cut/ent2idx.json", "w") as f:
        json.dump(entities, f, indent=2)

    with open(f"{base_dir}-cut/rel2idx.json", "w") as f:
        json.dump(relations, f, indent=2)

    # dump the cut Entity-Linking data
    el_data = [item for item in el_data if item["correct_id"] in entities]
    with open(f"{base_dir}-cut/entity-linking/test.json", "w") as f:
        json.dump(el_data, f, indent=2)



        
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--entities", default="./corrected/entity_ids.txt")
    parser.add_argument("--descriptions", default="./corrected/descriptions.txt")
    parser.add_argument("--names", default="./corrected/labels.txt")
    parser.add_argument("--graph", default="./corrected/graph.txt")
    parser.add_argument("--wikipedia")

    args = parser.parse_args()

    base_dir = "./prepared_dataset"
    if args.wikipedia is not None:
        base_dir = f"{base_dir}_with_wikipedia"
    for s in ("", "-cut"):
        Path(f"{base_dir + s}/pretraining").mkdir(parents=True, exist_ok=True)
        Path(f"{base_dir + s}/link-prediction").mkdir(parents=True, exist_ok=True)
        Path(f"{base_dir + s}/entity-linking").mkdir(parents=True, exist_ok=True)
    
    descriptions = load(args.descriptions)
    names = load(args.names)
    entities = load_entities(args.entities)
    graph = load_graph(args.graph)
    relations = {triplet[1] for triplet in graph}
    if args.wikipedia is not None:
        with open(args.wikipedia, "r") as f:
            wikipedia = json.load(f)
        for k,v in wikipedia.items():
            if v is not None:
                descriptions.update({k: v})

    # generate some statistics for the descriptions
    desc_stats = {}
    for desc in descriptions.values():
        desc = desc[0]
        if desc in desc_stats:
            desc_stats[desc] += 1
        else:
            desc_stats[desc] = 1
    desc_stats = dict(sorted(desc_stats.items(), key=lambda x: x[1], reverse=True))
    with open("descriptions_stats.json", "w") as f:
        json.dump(desc_stats, f, indent=2)

    # load the dataset, prepare it and save it
    dataset = {}
    for _set in ("train", "dev", "test"):
        with open(f"corrected/wikidata-disambig-{_set}.json", "r") as f:
            data = json.load(f)
            if _set == "test":
                el_data = data.copy()
                with open(f"{base_dir}/entity-linking/test.json", "w") as f:
                    json.dump(el_data, f, indent=2)
        ents = extract_entities_from_disamb_data(data)
        dataset[_set] = prepare_pretraining_data(ents)
        
        with open(f"{base_dir}/pretraining/{_set}_original.json", "w") as f:
            json.dump(dataset[_set], f, indent=2)

    # prepare and save the entity and relation indices
    entities = dict(zip(entities, range(len(entities))))
    relations = dict(zip(relations, range(len(relations))))
    
    with open(f"{base_dir}/ent2idx.json", "w") as f:
        json.dump(entities, f, indent=2)

    with open(f"{base_dir}/rel2idx.json", "w") as f:
        json.dump(relations, f, indent=2)

    # generate and save a better split
    complete = dict([item for s in ("train", "dev", "test") for item in dataset[s].items()])
    new_train, new_dev_test = train_test_split(list(complete.items()), test_size=0.2, train_size=0.8)
    new_dev, new_test = train_test_split(new_dev_test, test_size=0.5, train_size=0.5)
    new_dataset = {"train": dict(new_train), "dev": dict(new_dev), "test": dict(new_test)}

    for _set in ("train", "dev", "test"):
        with open(f"{base_dir}/pretraining/{_set}.json", "w") as f:
            json.dump(new_dataset[_set], f, indent=2)

    # cut (and dump) the original dataset
    cut_dataset(dataset, graph, entities, base_dir, el_data)
    # cut (and dump) the better split dataset
    cut_dataset(dataset, graph, entities, base_dir, el_data)
