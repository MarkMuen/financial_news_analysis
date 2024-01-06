from transformers import AutoTokenizer, AutoModelForSequenceClassification, \
     TextClassificationPipeline

TOKENIZER_KWARGS = {'truncation': True}


def create_text_classification_pipeline(model_name) -> TextClassificationPipeline:
    """_summary_

    Returns:
        TextClassificationPipeline: _description_
    """
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    pipe = TextClassificationPipeline(model=model, tokenizer=tokenizer, top_k=None)
    return pipe
