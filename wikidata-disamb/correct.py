import sys, json, warnings
sys.path.append("../")

from get_descriptions_and_labels import load_entities

from pathlib import Path
from prepare import load


def replace_redirected_entities(data: dict | set | list):
    global redirections
    data_copy = data.copy()
    if isinstance(data, list) or isinstance(data, set):
        data_copy = dict(zip(data_copy, range(len(data_copy))))

    to_delete = []
    updates = {}
    for _id, item in data_copy.items():
        if redirections[_id] != "None":
            to_delete.append(_id)
            updates[redirections[_id]] = item
    [data_copy.pop(_id) for _id in to_delete]
    data_copy.update(updates)
    if isinstance(data, list) or isinstance(data, set):
        data_copy = data.__class__(data_copy)
    return data_copy


if __name__ == "__main__":

    Path("./corrected/").mkdir(parents=True, exist_ok=True)

    entity_ids = set(load_entities("original/entity_ids.txt"))
    redirections = load("redirections.txt", as_list=False)
    descriptions = load("original/descriptions.txt", as_list=False)
    labels = load("original/labels.txt", as_list=False)
    try:
        with open("original/missing_entities.json", "r") as f:
            missing_entities = json.load(f)
    except FileNotFoundError:
        warnings.warn("File: `original/missing_entities.json` not found, proceeding without it.")
        missing_entities = {}
        
    for entity, metadata in missing_entities.items():
        if redirections[entity] != "None":
            warnings.warn(f"Found an existing redirection for entity {entity}: {redirections[entity]}, overwriting it with {entity}: {metadata['alternative_id']}.")
        if labels[entity] != "None":
            warnings.warn(f"Found and existing label for entity {entity}: {labels[entity]}, overwriting it with {entity}: {metadata['label']}.")
        labels[entity] = metadata["label"]
        if descriptions[entity] != "None":
            warnings.warn(f"Found and existing description for entity {entity}\n{entity}: {labels[entity]}\noverwriting it with\n{entity}: {metadata['label']}.")
        descriptions[entity] = metadata["description"]
        
    # load the dataset
    dataset = {}
    for _set in ("train", "dev", "test"):
        with open(f"original/wikidata-disambig-{_set}.json", "r") as f:
            dataset[_set] = json.load(f)
            
    corrected_ents = replace_redirected_entities(entity_ids)
    corrected_desc = replace_redirected_entities(descriptions)
    corrected_labels = replace_redirected_entities(labels)

    non_existing_entities = [e for e, d in corrected_labels.items() if d == "None"]
    corrected_ents = [e for e in corrected_ents if e not in non_existing_entities]
    for entity in non_existing_entities:
        corrected_labels.pop(entity)
        corrected_desc.pop(entity)

    breakpoint()
