import argparse, json

def load(filename):
    _dict = {}
    with open(filename, "r") as f:
        for line in f.read().split("\n"):
            _id, data = line.split(" ")
            if _id in _dict:
                _dict[_id].append(data)
            else:
                _dict[_id] = [data]
    return _dict

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--entities")
    parser.add_argument("--descriptions")
    parser.add_argument("--names")

    args = parser.parse_args()

    descriptions = load(args.descriptions)
    names = load(args.names)
    
    with open(args.entities, "r") as f:
        entities = f.read().split("\n")
        
    prepared_data = {}
    for _id in entities:
        prepared_data[_id] = {
            "wikidata_id": _id,
            "caption": descriptions.get(_id, names.get(_id, None))[0],
            "entity_id": _id,
        }

    with open(f"{args.entities}_prepared", "w") as f:
        json.dump(prepared_data, f, indent=2)