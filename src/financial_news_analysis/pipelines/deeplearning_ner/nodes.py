import pandas as pd
from .utils import process_article_to_sents, perform_ner_annotation
from tqdm import tqdm

tqdm.pandas()


def create_sentence_df(df_news: pd.DataFrame) -> pd.DataFrame:
    """_summary_

    Args:
        df_news (pd.DataFrame): _description_

    Returns:
        pd.Dataframe: _description_
    """
    sents = df_news.progress_apply(process_article_to_sents, axis=1)
    sents = sents.to_list()

    return pd.concat(sents, ignore_index=True)


def create_ner_tags(df_news: pd.DataFrame) -> pd.DataFrame:
    """Collect NER annotations from Flair model
    https://huggingface.co/flair/ner-english-ontonotes-large

    Args:
        df_news (pd.DataFrame): Data Frame containing text

    Returns:
        pd.DataFrame: DataFrame with annotations
    """

    annos = df_news.progress_apply(perform_ner_annotation, axis=1)
    annos = annos.to_list()
    return pd.concat(annos, ignore_index=True)
