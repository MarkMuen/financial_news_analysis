"""
This is a boilerplate pipeline 'match_deeplearning_ner_annoations'
generated using Kedro 0.18.5
"""
import logging
from .utils import calculate_matches, combine_name_matches, \
    set_column_names, format_match_data, clean_names
import pandas as pd
from tqdm import tqdm


tqdm.pandas()

ALLOWED_ANNOATION_TYPES = ["ORG"]


def filter_ner_annotations(df_annotations: pd.DataFrame) -> pd.DataFrame:
    """Filter NER annotations to only include allowed types

    Args:
        df_annotations (pd.DataFrame): Dataframe with NER annotations

    Returns:
        pd.DataFrame: Dataframe with filtered NER annotations
    """
    return df_annotations[df_annotations["label"].isin(ALLOWED_ANNOATION_TYPES)]


def clean_ner_annotations(df_annotations: pd.DataFrame) -> pd.DataFrame:
    """ Clean the annotations texts from the NER model

    Args:
        df_annotations (pd.DataFrame): Dataframe with annotations texts from NER model

    Returns:
        pd.DataFrame: Dataframe with cleaned annotations texts
    """
    df_annotations["text_cleaned"] = df_annotations["text"].apply(clean_names)
    return df_annotations


def ner_ticker_matching(df_ner: pd.DataFrame, df_ticker: pd.DataFrame) -> pd.DataFrame:
    """ Match NER annotations with ticker data by using the ticker name
    and annotation text

    Args:
        df_ner (pd.DataFrame): Dataframe with NER annotations
        df_ticker (pd.DataFrame): Dataframe with ticker data

    Returns:
        pd.DataFrame: Dataframe with matches between NER annotations and ticker data
    """
    logging.info("Matching NER annotations with ticker data")
    df_matches = df_ner["text_cleaned"].progress_apply(
        lambda x: calculate_matches(x, df_ticker)
        )

    logging.info("Combining name matches")
    df_matches = df_matches.progress_apply(
        lambda x: combine_name_matches(x)
        )

    logging.info("Formatting match data")
    df_matches = set_column_names(df_matches, df_ticker)
    df_matches = format_match_data(df_matches)

    return df_matches
