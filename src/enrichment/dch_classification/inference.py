import torch
from transformers import BertForSequenceClassification, BertTokenizer

# Load the saved model
model_path = "models/bert_classifier"
model = BertForSequenceClassification.from_pretrained(model_path)
tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")

device = "mps" if torch.backends.mps.is_available() else "cpu"
model.to(device)
model.eval()


def predict(texts):
    """
    Predict labels for a list of texts

    Args:
        texts: List of strings or single string

    Returns:
        predictions: List of predicted labels (0 or 1)
        probabilities: List of probability scores
    """
    if isinstance(texts, str):
        texts = [texts]

    # Tokenize
    encoded = tokenizer(
        texts, padding=True, truncation=True, max_length=512, return_tensors="pt"
    )

    input_ids = encoded["input_ids"].to(device)
    attention_mask = encoded["attention_mask"].to(device)

    # Predict
    with torch.no_grad():
        outputs = model(input_ids, attention_mask=attention_mask)
        logits = outputs.logits
        probabilities = torch.softmax(logits, dim=1)

    predictions = torch.argmax(probabilities, dim=1).cpu().numpy()
    probs = probabilities.cpu().numpy()

    return predictions, probs


# Example usage
sample_texts = [
    # """Objective Cultural heritage (CH) digitisation brought great opportunities to preserve, maintain and promote it. Yet, it also triggers challenges in terms of representation and content exhibition. This becomes particularly pressing in the context of CH of minorities. Overall, this reduces participation and inclusion of minorities, hindering equitable representations of diverse values in digitisation, leading to increased risks of misuse of digital CH. DIGICHer tackles these challenges by providing new understanding on key legal and policy, socio-""",
    """ """,
    """DIGICHer-Digitisation of cultural heritage of minority communities for equity and renewed engagement""",
    # """A Roadmap to the realization of fusion energy was adopted by the EFDA system at the end of 2012. The roadmap aims at achieving all the necessary know-how to start the construction of a demonstration power plant (DEMO) by 2030, in order to reach the goal of fusion electricity in the grid by 2050. The roadmap has been articulated in eight different Missions. The present proposal has the goal of implementing the activities described in the Roadmap during Horizon 2020 through a joint programme of the members of the EUROfusion Consortium. ITER is the key facility in the roadmap. Thus, ITER success remains the most important overarching objective of the programme and, in th"""
    """EUROfusion-Implementation of activities described in the Roadmap to Fusion during Horizon 2020 through a Joint programme of the members of the EUROfusion consortium""",
]


predictions, probabilities = predict(sample_texts)

for text, pred, prob in zip(sample_texts, predictions, probabilities):
    print(f"Text: {text}")
    print(f"Prediction: {pred} {'CH' if pred == 1 else 'NOT CH'}")
    print(f"Probabilities: Class 0: {prob[0]:.4f}, Class 1: {prob[1]:.4f}")
    print()
