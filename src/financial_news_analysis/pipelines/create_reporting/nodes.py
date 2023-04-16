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
                                                    'article_id',
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

    return df_data
