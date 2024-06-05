import sys, json, random, warnings, re

from transformers import AutoTokenizer
sys.path.append("../")

from get_descriptions_and_labels import load_entities, dump

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


def update_dataset(data):
    global non_existing_entities, corrected_ents
    
    new_data = []
    for i, sample in enumerate(data):
        ids = [sample["correct_id"], sample["wrong_id"]]
        if ids[0] in non_existing_entities:
            continue
        new_ids = replace_redirected_entities(ids)
        if len(new_ids) < 2:
            # the correct id and wrong id were redirected to the same entity id
            # put as wrong id another random one
            new_ids.append(
                random.choice(
                    list(set(corrected_ents) - {new_ids[0]})
                )
            )
        if ids[1] in non_existing_entities:
            new_ids[1] = random.choice(
                list(set(corrected_ents) - {new_ids[0]})
            )
        sample["correct_id"] = new_ids[0]
        sample["wrong_id"] = new_ids[1]
        new_data.append(sample)
    return new_data


def find_entity_span(entity_mention, entity, allow_recursion=True):
    global tokenizer

    l = entity.shape[-1]
    i = 0
    ent = tokenizer.decode(entity.ravel())
    while i + l <= entity_mention.shape[-1]:
        if all(entity_mention[0][i:i+l] == entity[0]):
            return (i, i+l), ent
        i += 1
    # try with a space in front
    if ent[0] != " " and allow_recursion:
        ent = tokenizer(f" {ent.lower()}", add_special_tokens=False, return_tensors="pt").input_ids
        return find_entity_span(entity_mention, ent)
    # some labels don't precisely coincide with the words in the text
    else:
        # they miss the final s, n or ed for instance
        desinences = ("s", "n", f"{ent[-1]}ed", "ic", "en", "es", "ns", "er", "ation", "ing", "ed", f"{ent[-1]}ing", "al", "\"")
        for desinence in desinences:
            if ent[-1] != desinence and allow_recursion:
                span = find_entity_span(
                    entity_mention,
                    tokenizer(f"{ent}{desinence}", add_special_tokens=False, return_tensors="pt").input_ids,
                    False
                )
                if span is not None:
                    return span, f"{ent}{desinence}"


def fix_string_label(sample):
    global tokenizer
    entity = sample["string"]
    entity_mention = sample["text"]
    # edit the sentence to help the tokenizer
    # insert white space between contiguos punctuation: ., -> . ,
    entity_mention = re.sub("(?<=[.,:;])(?=[.,:;])", r"\g<0> ", entity_mention.lower())
    # insert white space in expression between apices: "xxx" -> " xxx "
    entity_mention = re.sub("(?<=\s[\"])[^\"]+(?=[\"]\s)", r" \g<0> ", entity_mention)
    if entity_mention[0] != " ":
        entity_mention = f" {entity_mention}"
    sample["text"] = entity_mention
    tokenized_mention = tokenizer(entity_mention, add_special_tokens=False, return_tensors="pt")
    tokenized_entity = tokenizer(entity.lower(), add_special_tokens=False, return_tensors="pt")
    span, updated_label = find_entity_span(tokenized_mention.input_ids, tokenized_entity.input_ids)
    sample["string"] = updated_label
    return sample
    

if __name__ == "__main__":

    Path("./corrected/").mkdir(parents=True, exist_ok=True)

    tokenizer = AutoTokenizer.from_pretrained("gpt2")
    
    # load the dataset
    dataset = {}
    for _set in ("train", "dev", "test"):
        with open(f"original/wikidata-disambig-{_set}.json", "r") as f:
            dataset[_set] = json.load(f)
    
    entity_ids = set(load_entities("original/entity_ids.txt"))
    redirections = load("redirections.txt", as_list=False)
    descriptions = load("original/descriptions.txt", as_list=False)
    labels = load("original/labels.txt", as_list=False)

    # manually labelled missing entities
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

    # correct entity ids, labels and descriptions
    corrected_ents = replace_redirected_entities(entity_ids)
    corrected_desc = replace_redirected_entities(descriptions)
    corrected_labels = replace_redirected_entities(labels)

    # discarding non-existent entities
    non_existing_entities = {e for e, d in corrected_labels.items() if d == "None"}
    corrected_ents = [e for e in corrected_ents if e not in non_existing_entities]
    for entity in non_existing_entities:
        corrected_labels.pop(entity)
        corrected_desc.pop(entity)

    # --> find a way to deal with missing descriptions or meaningless "Wikimedia disambiguation page" descriptions
    breakpoint()
    dump("corrected/entity_ids.txt", corrected_ents)
    dump("corrected/labels.txt", *zip(*corrected_labels.items()))
    dump("corrected/descriptions.txt", *zip(*corrected_desc.items()))    
    
    for _set in ("train", "dev", "test"):
        with open(f"corrected/wikidata-disambig-{_set}.json", "w") as f:
            json.dump(update_dataset(dataset[_set]), f, indent=2)
        
