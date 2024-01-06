from typing import List
from dataclasses import dataclass
import pandas as pd
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForSequenceClassification, \
     TextClassificationPipeline


@dataclass
class Sentiment():
    """Dataclass for sentiment
    """
    sen_id: int
    source: str
    sentiment: str
    confidence: float


def combine_data_frames(df_general: pd.DataFrame,
                        df_finance: pd.DataFrame) -> pd.DataFrame:
    """_summary_

    Args:
        df_general (pd.DataFrame): _description_
        df_finance (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    return pd.concat([df_general, df_finance])


def get_sentiment_from_sentences(texts: List[str],
                                 pipe: TextClassificationPipeline,
                                 model_name: str) -> List[Sentiment]:
    """_summary_

    Args:
        texts (List[str]): List of texts to be analyzed
        pipe (TextClassificationPipeline): Transformer pipeline for sentiment analysis
        model_name (str): Name of the model to be used

    Returns:
        List[Sentiment]: List of dataclass elements with sentiment information
    """
    data = []

    for id, text in enumerate(tqdm(texts)):
        sentiment = pipe(text)
        for prediction in sentiment[0]:
            data.append(Sentiment(sen_id=id,
                                  source=model_name,
                                  sentiment=prediction["label"],
                                  confidence=prediction["score"]
                                  )
                        )
    return data


def create_text_classification_pipeline(model_name) -> TextClassificationPipeline:
    """_summary_

    Returns:
        TextClassificationPipeline: _description_
    """
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    pipe = TextClassificationPipeline(model=model, tokenizer=tokenizer, top_k=None)
    return pipe


def get_sentiment_from_df(df_sentence: pd.DataFrame,
                          col: str,
                          model_name: str) -> pd.DataFrame:
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        col (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    data = get_sentiment_from_sentences(df_sentence[col].tolist(),
                                        create_text_classification_pipeline(model_name),
                                        model_name)
    return pd.DataFrame(data)


def filter_max_confidence_sentiment(df_sents: pd.DataFrame) -> pd.DataFrame:
    """_summary_

    Args:
        df_sents (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    df_sents_filtered = df_sents.sort_values(["confidence"], ascending=True) \
        .groupby(["sen_id", "source"]) \
        .tail(1)
    return df_sents_filtered


def create_sentiment_statistics(df_sents: pd.DataFrame) -> pd.DataFrame:
    """_summary_

    Args:
        df_sents (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    stats = df_sents.groupby(["source", "sentiment"])["sen_id"]\
        .count().reset_index(name="count")
    return stats


def add_article_data(df_sents_filtered: pd.DataFrame, df_article: pd.DataFrame):
    """_summary_

    Args:
        df_sents_filtered (pd.DataFrame): _description_
        df_article (pd.DataFrame): _description_

    Returns:
        _type_: _description_
    """
    df_sents_w_article = df_sents_filtered.merge(df_article,
                                                 how='inner',
                                                 left_on='sen_id',
                                                 right_index=True)
    df_sents_w_article = df_sents_w_article.drop(columns=["sentence"])
    return df_sents_w_article
