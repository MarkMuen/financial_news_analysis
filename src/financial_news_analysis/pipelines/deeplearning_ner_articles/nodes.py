import pandas as pd
from .utils import process_article_to_sents, perform_ner_annotation
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
            lambda row: process_article_to_sents(row, "article", sents_num), axis=1
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
    tagger = SequenceTagger.load("flair/ner-english-ontonotes-large")
    annos = df_news.progress_apply(
            lambda row: perform_ner_annotation(row, tagger), axis=1
        )
    annos = annos.to_list()
    return pd.concat(annos, ignore_index=True)
