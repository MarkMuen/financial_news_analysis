"""
This is a boilerplate pipeline 'create_reporting'
generated using Kedro 0.18.5
"""
import pandas as pd


def add_article_data(ner_ticker_matches: pd.DataFrame,
                     df_ek_rh_ticker: pd.DataFrame,
                     df_annos: pd.DataFrame,
                     df_news: pd.DataFrame) -> pd.DataFrame:
    """Node for adding article data to the dataframe

    Args:
        df (pandas.DataFrame): Dataframe with the data from the database
        article_data (dict): Data from the article

    Returns:
        pandas.DataFrame: Dataframe with the article data added
    """

    ner_ticker_matches = ner_ticker_matches.merge(df_ek_rh_ticker,
                                                  how='inner',
                                                  right_on='ticker_cleaned',
                                                  left_on='ticker_name')
    ner_ticker_matches = ner_ticker_matches[['ner_id', 'ticker_name']]

    ner_ticker_matches = ner_ticker_matches.merge(df_annos,
                                                  how='inner',
                                                  left_on='ner_id',
                                                  right_index=True)

    df_news = df_news[["date", "year", "month", "day", "publication"]]
    ner_ticker_matches = ner_ticker_matches.merge(df_news,
                                                  how='inner',
                                                  left_on='article_id',
                                                  right_index=True)

    return ner_ticker_matches


def drop_id_columns(df_matched_data: pd.DataFrame) -> pd.DataFrame:
    """Node for dropping the id columns from the dataframe

    Args:
        df_matched_data (pandas.DataFrame): Dataframe with the data

    Returns:
        pandas.DataFrame: Dataframe with the id columns dropped
    """
    df_matched_data = df_matched_data.drop(columns=['ner_id',
                                                    'sentence_id',
                                                    'sentence_nr'])
    return df_matched_data


def clean_data_types(df_data: pd.DataFrame) -> pd.DataFrame:
    """Node for cleaning the data types of the dataframe

    Args:
        df_data (pandas.DataFrame): Dataframe with the data

    Returns:
        pandas.DataFrame: Dataframe with the data types cleaned"""

    df_data["year"] = df_data["year"].astype(int)
    df_data["month"] = df_data["month"].astype(int)
    df_data["day"] = df_data["day"].astype(int)
    df_data["hour"] = df_data["date"].apply(lambda x: pd.to_datetime(x).hour) \
        .astype(int)
    df_data["minute"] = df_data["date"].apply(lambda x: pd.to_datetime(x).minute) \
        .astype(int)
    df_data["second"] = df_data["date"].apply(lambda x: pd.to_datetime(x).second) \
        .astype(int)

    return df_data


def compare_ner_annotations(ner_ticker_matches: pd.DataFrame,
                            ner_ticker_matching_full_text: pd.DataFrame,
                            ner_anntoations: pd.DataFrame,
                            ner_anntoations_full_text: pd.DataFrame) -> pd.DataFrame:
    """Compare NER annotations from limited and full text

    Args:
        ner_ticker_matches (pd.DataFrame): NER annotations from limited text
        ner_ticker_matching_full_text (pd.DataFrame): NER annotations from full text
        ner_anntoations (pd.DataFrame): Annotations from limited text
        ner_anntoations_full_text (pd.DataFrame): Annotations from full text

    Returns:
        pd.DataFrame: List of NER annotations that are only in full text
    """

    ner_annos_enriched = ner_ticker_matches.merge(
        ner_anntoations,
        left_on="ner_id",
        right_index=True,
        how="inner")

    ner_annos_enriched_full_text = ner_ticker_matching_full_text.merge(
        ner_anntoations_full_text,
        left_on="ner_id",
        right_index=True,
        how="inner"
    )
    relevant_cols = ["ticker_name", "article_id"]
    ner_annos_enriched = ner_annos_enriched[relevant_cols].drop_duplicates()
    ner_annos_enriched_full_text = ner_annos_enriched_full_text[relevant_cols] \
        .drop_duplicates()
    matched_ner_annos = ner_annos_enriched_full_text.merge(
        ner_annos_enriched,
        on=["article_id", "ticker_name"],
        how="left",
        indicator=True
    )
    matched_ner_annos = matched_ner_annos[
        matched_ner_annos["_merge"] == "left_only"
    ]
    matched_ner_annos = matched_ner_annos.drop_duplicates()
    matched_ner_annos["category"] = "full_text_only"
    return matched_ner_annos.drop(columns="_merge")


def calulate_overlap(df_ner_comparison: pd.DataFrame,
                     df_ner_full_text: pd.DataFrame,
                     ner_anntoations_full_text: pd.DataFrame) -> pd.DataFrame:
    """Calculate overlap of NER annotations between limited and full text

    Args:
        df_ner_comparison (pd.DataFrame): NER annotations in both limited
            text and full text
        df_ner_full_text (pd.DataFrame): NER annotations from full text
    Returns:
        pd.DataFrame: Overlap of NER annotations between limited and full text
    """
    df_ner_full_text_enriched = df_ner_full_text.merge(
        ner_anntoations_full_text,
        left_on="ner_id",
        right_index=True,
        how="inner"
    )

    relevant_cols = ["ticker_name", "article_id"]
    df_ner_comparison = df_ner_comparison[relevant_cols].drop_duplicates()
    df_ner_full_text_enriched = df_ner_full_text_enriched[relevant_cols] \
        .drop_duplicates()

    sample_size = df_ner_full_text_enriched.shape[0]
    un_matched_size = df_ner_comparison.shape[0]
    matched_size = sample_size - un_matched_size
    overlap = matched_size / sample_size
    result = pd.DataFrame([{"sample_size": sample_size,
                            "matched_size": matched_size,
                            "un_matched_size": un_matched_size,
                            "measure": "overlap",
                            "value": overlap}])

    return result
