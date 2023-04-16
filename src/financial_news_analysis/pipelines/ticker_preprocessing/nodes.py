import string
from typing import List
from nltk.tokenize import word_tokenize
from cleanco import basename
import pandas as pd


FORBIDDES_NAME_PARTS = {".com": "",
                        "cl.": "class ",
                        "europacific partners": "",
                        "t-mobile us": "t-mobile",
                        "(nas)": "",
                        "(xsc)": "",
                        "(nasdaq non-national)": "",
                        "equinix reit": "equinix",
                        "meta platforms": "meta",
                        "cisco systems": "cisco",
                        "Global Holdings": "holdings"
                        }

FORBIDDEN_NAME_TOKENS = ["group",
                         "class",
                         "keurig",
                         "technology",
                         "international",
                         "pharmaceuticals",
                         "holdings",
                         "holding",
                         "company",
                         "solutions"]

ALTERNATIVE_NAMES = {"meta": "facebook",
                     "alphabet": "google"}


def clean_names(name: str) -> str:
    """Clean entity names for better matching

    Args:
        name (str): raw entity name

    Returns:
        str: clean name of entity
    """
    if name:
        name = name.lower()

        for k, v in FORBIDDES_NAME_PARTS.items():
            name = name.replace(k, v)

        name = basename(name)

        tokens = word_tokenize(name)
        tokens = [t for t in tokens if len(t) > 1]
        tokens = [t for t in tokens if t not in FORBIDDEN_NAME_TOKENS]
        tokens = [t for t in tokens if t not in string.punctuation]
        return " ".join(t for t in tokens)
    else:
        return name


def cast_to_string_type(df_ek: pd.DataFrame) -> pd.DataFrame:
    """Cast all columns to string type

    Args:
        df_ek (pd.DataFrame): Data with columns

    Returns:
        pd.DataFrame: Dataframe with all columns casted to string type
    """

    for c in df_ek.columns:
        df_ek[c] = df_ek[c].astype(str)
    return df_ek


def clean_ticker(row: pd.Series, t_col: str, s_col: str) -> str:
    """ Fill missing ticker with preprocessed symbol data

    Args:
        row (pd.Series): Row of a ticker data set
        t_col (str): name of the ticker column
        s_col (str): name of the symbol column

    Returns:
        str: Unified ticker
    """

    if (row[t_col]) and (row[t_col] != "nan") and (str(row[t_col]).lower() != "none"):
        ticker = str(row[t_col])
    else:
        if (row[s_col]) and (row[s_col] != "nan") \
                and (str(row[s_col]).lower() != "none"):
            ticker = str(row[s_col]).replace("@", "").replace("U:", "")
        else:
            return None
    return ticker


def remove_not_needed_ticker_lines(df_ticker: pd.DataFrame) -> pd.DataFrame:
    """Clean ticker data from not needed lines e.g. empty tickers and duplicates

    Args:
        df_ticker (pf.DataFrame): Ticker Data in data frame

    Returns:
        pd.DataFrame: Ticker Data without duplicates and empty tickers
    """
    df_ticker = df_ticker.dropna(subset=["ticker"])
    df_ticker = df_ticker.drop_duplicates(subset=["ticker"])
    return df_ticker


def clean_name_per_col(df_ticker: pd.DataFrame, col: str) -> None:
    """Apply cleaning of entity names

    Args:
        df_ticker (pd.DataFrame): Ticker data
        col (str): Column name to be cleaned
    """
    df_ticker[f"{col}_cleaned"] = df_ticker[col].apply(clean_names)


def clean_names_in_df(df_ticker: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Clean names in ticker data

    Args:
        df_ticker (pd.DataFrame): Ticker Data in data frame

    Returns:
        pd.DataFrame: Dataframe with names cleaned
    """
    df_ticker["ticker_cleaned"] = df_ticker["ticker"].apply(lambda x: x.lower())
    df_ticker = df_ticker.fillna("")
    df_ticker = cast_to_string_type(df_ticker)
    for c in cols:
        clean_name_per_col(df_ticker, c)

    return df_ticker


def determine_alternative_name(name_short: str, name_long: str) -> str:
    """Search for all styles of a company name if an alternative name exists

    Args:
        row (pd.Series): Row of a Dataframe that contains all styles of a name

    Returns:
        str: Alternative company name
    """

    if name_short in ALTERNATIVE_NAMES.keys():
        return ALTERNATIVE_NAMES[name_short]
    elif name_long in ALTERNATIVE_NAMES.keys():
        return ALTERNATIVE_NAMES[name_long]
    else:
        return None


def search_alternative_name(df_ticker: pd.DataFrame) -> pd.DataFrame:
    """Add alternative names to ticker df

    Args:
        df_ticker (pd.DataFrame): Ticker data with cleaned names

    Returns:
        pd.DataFrame: Ticker data with alternative names added
    """
    df_ticker["alternative_name"] = df_ticker.apply(
            lambda row: determine_alternative_name(row["name_cleaned"],
                                                   row["full_name_cleaned"]),
            axis=1
        )
    return df_ticker


def select_relevant_cols_ek_ticker(df_ticker: pd.DataFrame) -> pd.DataFrame:
    """Clean column names for ticker data

    Args:
        df_ticker (pd.DataFrame): Ticker Data

    Returns:
        pd.DataFrame: Subset of ticker data with cleaned column names
    """
    df_ticker["Ticker_list"] = df_ticker.apply(
        lambda row: clean_ticker(row, "Ticker_list", "Symbol"),
        axis=1)
    df_ticker = df_ticker[["Name", "Full Name", "Ticker_list"]]
    df_ticker = df_ticker.rename(columns={"Name": "name",
                                          "Full Name": "full_name",
                                          "Ticker_list": "ticker"})

    df_ticker = remove_not_needed_ticker_lines(df_ticker)

    return df_ticker


def select_relevant_cols_rh(df_rh: pd.DataFrame) -> pd.DataFrame:
    """Drop irrelevant columns from robin hood ticker

    Args:
        df_rh (pd.DataFrame): Robin hood ticker data

    Returns:
        pd.DataFrame: Robin hood ticker with reduced columns
    """
    df_rh = df_rh.drop(columns=["Unnamed: 0"])
    df_rh = df_rh.rename(columns={"Ticker": "ticker"})
    return df_rh


def join_ticker_data_sources(df_ek: pd.DataFrame, df_rh: pd.DataFrame) -> pd.DataFrame:
    """Join ticker data from Robin Hood and Eikon datasets

    Args:
        df_ek (pd.DataFrame): Eikon ticker data
        df_rh (pd.DataFrame): Robin hood ticker data

    Returns:
        pd.DataFrame: Joined ticker data from both sources
    """
    df_ticker_joined = df_ek.merge(df_rh, how="inner", on=["ticker"])
    return df_ticker_joined[["ticker_cleaned", "ticker", "name", "full_name"]]


def select_relevant_names_ek_names(df_ek: pd.DataFrame) -> pd.DataFrame:
    """ Select relevant columns from Eikon names data

    Args:
        df_ek (pd.DataFrame): Eikon names data

    Returns:
        pd.DataFrame: Eikon names data with relevant columns
    """
    df_ek = df_ek[["Symbol", "Ticker", "Name1", "Name2", "Name3", "Name4", "Name5"]]
    df_ek = df_ek.rename(columns={"Symbol": "symbol", "Ticker": "ticker",
                                  "Name1": "add_name_1", "Name2": "add_name_2",
                                  "Name3": "add_name_3", "Name4": "add_name_4",
                                  "Name5": "add_name_5"})
    return df_ek


def preprocess_ek_names_data(df_ek: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicates and add ticker to Eikon Names data

    Args:
        df_ek (pd.DataFrame): Eikon names data

    Returns:
        pd.DataFrame: Preprocessed eikon names data
    """
    df_ek["ticker"] = df_ek.apply(
        lambda row: clean_ticker(row, "ticker", "symbol"),
        axis=1)
    df_ek = df_ek.drop(columns=["symbol"])
    df_ek = remove_not_needed_ticker_lines(df_ek)

    return df_ek


def merge_ek_data(df_ek_ticker: pd.DataFrame,
                  df_ek_names: pd.DataFrame) -> pd.DataFrame:
    """Merge Eikon ticker and names data

    Args:
        df_ek_ticker (pd.DataFrame): Eikon ticker data
        df_ek_names (pd.DataFrame): Eikon names data

    Returns:
        pd.DataFrame: Merged Eikon ticker and names data
    """
    df_ek = df_ek_ticker.merge(df_ek_names.drop(columns=["ticker"]),
                               how="outer",
                               on=["ticker_cleaned"])
    return df_ek
