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


def merge_ner_anntotations(df_ner_header: pd.DataFrame,
                           df_ner_article: pd.DataFrame) -> pd.DataFrame:
    """Merge Header and Article NER Annotations DataFrames and add source column

    Args:
        df_ner_header (pd.DataFrame): Ner Annotations for header data
        df_ner_article (pd.DataFrame): Ner Annotations for article data

    Returns:
        pd.DataFrame: Ner Annotations for header and article data
    """
    logging.info("Merging NER annotations")
    df_ner_header["source"] = "header"
    df_ner_article["source"] = "article"
    df_ner = pd.concat([df_ner_header, df_ner_article], ignore_index=True)
    return df_ner


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


def seperate_ner_annotations(df_ner: pd.DataFrame,
                             lower_percentage: float,
                             upper_percentage: float) -> pd.DataFrame:
    """Cut anntionations by percentage into smaller sub frames

    Args:
        df_ner (pd.DataFrame): Inital DataFrame with annotations
        lower_percentage (float): Lower Percentage for cutting data
        upper_percentage (float): Upper Percentage for cutting data

    Returns:
        pd.DataFrame: Subset of Ner Data
    """
    length_ner = df_ner.shape[0]
    if lower_percentage != 0.0:
        lower_bound = int(length_ner * lower_percentage)
    else:
        lower_bound = 0
    if upper_percentage != 1.0:
        upper_bound = int(length_ner * upper_percentage)
    else:
        upper_bound = length_ner

    df_ner_sub = df_ner[lower_bound:upper_bound]
    print(f"Length NER-Frame: {length_ner}")
    print(f"Lower Bound: {lower_bound}")
    print(f"Upper Bound: {upper_bound}")
    print(f"Length NER-Sub-Frame: {df_ner_sub.shape[0]}")
    return df_ner_sub
