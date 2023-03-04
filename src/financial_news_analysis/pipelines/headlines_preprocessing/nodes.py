import datetime
import pandas as pd

from .utils import preprocess_string

VALS_TO_REPLACE = {"Feb": "February", "Jan": "January", "Dec": "December",
                   "Nov": "November", "Oct": "October", "Sept": "September",
                   "Aug": "August"}
TEXT_COLS = ["headline", "description"]


def clean_headline_texts(headlines: pd.DataFrame) -> pd.DataFrame:
    """Clean all relevant text columns in the headline data

    Args:
        headlines (pd.DataFrame): Dataframe containing headline data

    Returns:
        pd.DataFrame: Dataframe with cleaned headline data
    """
    for c in TEXT_COLS:
        headlines[c] = headlines[c].apply(lambda x: preprocess_string(x))

    return headlines


def add_source_as_col(df_heads: pd.DataFrame,
                      param: str) -> pd.DataFrame:
    """Add data source to df

    Args:
        df_heads (pd.DataFrame): Dataframe containing header data
        param (str): source name

    Returns:
        pd.DataFrame: Dataframe with source added
    """

    df_heads["data_source"] = param
    return df_heads


def merge_headline_dfs(df_c: pd.DataFrame,
                       df_g: pd.DataFrame,
                       df_r: pd.DataFrame) -> pd.DataFrame:
    """Merge all three dataframes containing headline data

    Args:
        df_c (pd.DataFrame): CNBC Dataset
        df_g (pd.DataFrame): Guardian Dataset
        df_r (pd.DataFrame): Reuters Dataset

    Returns:
        pd.DataFrame: Merged Dataset
    """

    return pd.concat([df_c, df_g, df_r], ignore_index=True)


def clean_month_desc(df: pd.DataFrame, param: str) -> pd.DataFrame:
    """_summary_

    Args:
        df (pd.DataFrame): Pandas Dataframe with Columns to be formatted
        param (str): Column name of date column

    Returns:
        pd.DataFrame: DataFrame with harmonized date columns
    """
    col = param

    for k, v in VALS_TO_REPLACE.items():
        df[col] = df[col].str.replace(k, v)

    return df


def preprocess_cnbc_headlines(df_cnbc: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess CNBC Headlines Dataset

    Parameters
    ----------
    df_cnbc : pandas DataFrame containing raw data

    Returns
    ----------
    df_cnbc : pandas DataFrame containing preprocessed data

    """
    df_cnbc = df_cnbc.dropna()
    df_cnbc = df_cnbc.rename(columns={"Headlines": "headline",
                                      "Time": "date_time",
                                      "Description": "description"})

    df_cnbc["date"] = df_cnbc["date_time"].apply(
        lambda x: datetime.datetime.strptime(x.split(",")[1].strip(),
                                             "%d %B %Y"
                                             ).strftime("%Y-%m-%d")
    )

    return df_cnbc


def preprocess_guardian_headlines(df_g: pd.DataFrame) -> pd.DataFrame:
    """Preprocess Guardian Data

    Args:
        df_g (pd.DataFrame): Pandas Dataframe containing Guardian Information

    Returns:
        pd.DataFrame: Preprocessed Guardian Headlines
    """
    df_g = df_g.dropna()
    df_g = df_g.rename(columns={"Headlines": "headline", "Time": "date_time"})
    df_g["description"] = df_g["headline"]
    df_g = df_g[df_g["description"].str.len() < 8]
    df_g["date"] = df_g["date_time"].apply(
        lambda x: datetime.datetime.strptime(x,
                                             "%d-%b-%y"
                                             ).strftime("%Y-%m-%d")
    )

    return df_g


def preprocess_reuters_headlines(df_r: pd.DataFrame) -> pd.DataFrame:
    """Preprocessing of reuters data

    Args:
        df_r (pd.DataFrame): dataframe contraining reuters data

    Returns:
        pd.DataFrame: preprocessed reuters data
    """
    df_r = df_r.dropna()
    df_r = df_r.rename(columns={"Headlines": "headline", "Time": "date_time",
                                "Description": "description"})

    df_r["date"] = df_r["date_time"].apply(
        lambda x: datetime.datetime.strptime(x, "%b %d %Y").strftime("%Y-%m-%d")
    )

    return df_r
