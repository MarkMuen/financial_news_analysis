import pandas as pd
from .utils import preprocess_string

TEXT_COLS = ["title", "article"]


def create_stats_time(df_news: pd.DataFrame) -> pd.DataFrame:
    """Create distribution over time for news articles

    Args:
        df_news (pd.DataFrame): Dataframe with news data

    Returns:
        pd.DataFrame: Distribution of articles over time
    """
    df_stats = df_news[['year', 'month', 'title']]\
        .groupby(['year', 'month'])\
        .count()\
        .sort_values(['year', 'month'])

    return df_stats


def create_stats_publisher(df_news: pd.DataFrame) -> pd.DataFrame:
    """Create Statistic for Publishers

    Args:
        df_news (pd.DataFrame): News data 

    Returns:
        pd.DataFrame: Descreptive statisics for publisher
    """
    df_stats = df_news[['publication', 'title']].groupby(['publication']).count()
    return df_stats


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
    """Add text column as concat of title and article

    Args:
        df_news (pd.DataFrame): News Dataframe containing
        tile and article

    Returns:
        pd.DataFrame: Dataframe with text column
    """
    df_news["text"] = df_news.apply(
            lambda r: r[TEXT_COLS[0]]+". "+r[TEXT_COLS[1]],
            axis=1
        )
    df_news["text_cleaned"] = df_news.apply(
            lambda r: r[f"{TEXT_COLS[0]}_cleaned"]+". "+r[f"{TEXT_COLS[1]}_cleaned"],
            axis=1
        )
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
        df_news[f"{c}_cleaned"] = df_news[c].apply(lambda x: preprocess_string(x))
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
