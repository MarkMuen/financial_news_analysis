import pandas as pd
from ..utils import preprocess_string

TEXT_COLS = ["title", "article"]


def preprocess_data(df_news: pd.DataFrame) -> pd.DataFrame:
    """Preprocess all the news dataset

    Args:
        df_news (pd.DataFrame): Raw data all the news

    Returns:
        pd.DataFrame: Preprocessed all the news data
    """
    df_news = df_news.rename(columns={"date": "date_time"})
    df_news = df_news.drop(['section', "author"], axis=1)
    mask = df_news["article"].isnull()
    df_news.loc[mask, "article"] = df_news.loc[mask, "title"]
    df_news["date"] = df_news["date_time"].apply(
        lambda x: x.split(" ")[0]
    )
    df_news = df_news[df_news["article"].notnull()]

    return df_news


def create_text_col(df_news: pd.DataFrame) -> pd.DataFrame:
    """Add text column as concat of tile and article

    Args:
        df_news (pd.DataFrame): News Dataframe containing 
        tile and article

    Returns:
        pd.DataFrame: Dataframe with text column
    """
    df_news["text"] = df_news.apply(lambda r: r["title"] + ". " + r["article"], axis=1)
    return df_news


def clean_texts(df_news: pd.DataFrame) -> pd.DataFrame:
    """Clean article texts

    Args:
        df_news (pd.DataFrame): Preprocessed all the news data

    Returns:
        pd.DataFrame: All the news data with cleaned texts
    """
    for c in TEXT_COLS:
        df_news[c] = df_news[c].astype(str)
        df_news[c] = df_news[c].apply(lambda x: preprocess_string(x))
    return df_news


def filter_news_data(df_news: pd.DataFrame, start_date: str) -> pd.DataFrame:
    """Filter all the news data to only contain releant data

    Args:
        df_news (pd.DataFrame): Cleaned all the news data
        start_date (str): Start date of analysis

    Returns:
        pd.DataFrame: Filtered news data to contain only relevant data
    """
    mask = df_news["date"] >= start_date

    return df_news[mask]
