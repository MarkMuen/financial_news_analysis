import os
import pandas as pd
from .utils import process_article_to_sents, perform_ner_annotation, \
    MODEL_NAME, MODEL_PATH
from tqdm import tqdm
from flair.models import SequenceTagger


tqdm.pandas()


def create_sentence_df(df_news: pd.DataFrame, sents_num: int = None) -> pd.DataFrame:
    """Separates the news dataset into a dataset of sentences

    Args:
        df_news (pd.DataFrame): Dataset containing news data

    Returns:
        pd.Dataframe: Dataset of sentences with reference to article
    """
    sents = df_news.progress_apply(
            lambda row: process_article_to_sents(row, "text", sents_num), axis=1
        )
    sents = sents.to_list()

    return pd.concat(sents, ignore_index=True)


def create_ner_tags(df_news: pd.DataFrame) -> pd.DataFrame:
    """Collect NER annotations from Flair model
    https://huggingface.co/flair/ner-english-ontonotes-large

    Args:
        df_news (pd.DataFrame): Data Frame containing sentences

    Returns:
        pd.DataFrame: DataFrame with annotations
    """

    p = os.path.join(MODEL_PATH, f"{MODEL_NAME}.pt")
    if os.path.exists(p):
        tagger = SequenceTagger.load(p)
    else:
        tagger = SequenceTagger.load(MODEL_NAME)

    annos = df_news.progress_apply(
            lambda row: perform_ner_annotation(row, tagger), axis=1
        )
    annos = annos.to_list()
    return pd.concat(annos, ignore_index=True)


def random_sample_news_data(df_news: pd.DataFrame, sample_size: float) -> pd.DataFrame:
    """Random sample news data from the complete data set.
    Distinguish between abolut number and percentage to be sampled.

    Args:
        df_news (pd.DataFrame): All the news data set
        sample_size (float): Percentage or number of sampled items

    Returns:
        pd.DataFrame: Random sample of news data
    """
    if sample_size > 1:
        print("Sample size is larger than 1, using absolute number of samples")
        sample_size = int(sample_size)
        df_news_sample = df_news.sample(n=sample_size, axis=0, random_state=2)
    elif sample_size < 1:
        print("Sample size is smaller than 1, using percentage of samples")
        df_news_sample = df_news.sample(frac=sample_size, axis=0, random_state=2)
    else:
        print("Sample size is 1, using all the data")
        df_news_sample = df_news.copy()

    return df_news_sample


def select_relevant_columns(df_news: pd.DataFrame) -> pd.DataFrame:
    """Filter All the news Data to relevant columns
    for all the news sentiment analysis

    Args:
        df_news (pd.DataFrame): All the news data

    Returns:
        pd.DataFrame: All the news with relevant columns
    """
    return df_news[["text"]]
