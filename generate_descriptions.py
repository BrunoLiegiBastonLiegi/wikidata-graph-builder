from langchain_community.llms import Ollama
from langchain.prompts import PromptTemplate


PROMPT = PromptTemplate.from_template("A Wikidata entity is provided below. Generate a short one-sentence long description of the entity.\nEntity: {entity}")

def generate_missing_description(entity: str, llm):
    breakpoint()
    return llm.invoke(PROMPT.format(entity=entity))


if __name__ == "__main__":
    llm = Ollama(model="llama2:13b")
    entity_labels = [id_to_label[i] for i in entity_ids]
    for i, d in enumerate(descriptions):
        if d is None or d == "None" or "Wikimedia" in d:
            print(entity_ids[i])
            descriptions[i] = generate_missing_description(entity_labels[i], llm)
