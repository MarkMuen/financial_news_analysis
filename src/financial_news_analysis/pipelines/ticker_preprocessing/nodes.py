import string
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
    name = name.lower()

    for k, v in FORBIDDES_NAME_PARTS.items():
        name = name.replace(k, v)

    name = basename(name)

    tokens = word_tokenize(name)
    tokens = [t for t in tokens if len(t) > 1]
    tokens = [t for t in tokens if t not in FORBIDDEN_NAME_TOKENS]
    tokens = [t for t in tokens if t not in string.punctuation]

    return " ".join(t for t in tokens)


def clean_name_per_col(df_ticker: pd.DataFrame, col: str) -> None:
    """Apply cleaning of entity names

    Args:
        df_ticker (pd.DataFrame): Ticker data
        col (str): Column name to be cleaned
    """
    df_ticker[f"{col}_cleaned"] = df_ticker[col].apply(clean_names)


def clean_names_in_df(df_ticker: pd.DataFrame) -> pd.DataFrame:
    """Clean names in ticker data

    Args:
        df_ticker (pd.DataFrame): Ticker Data in data frame

    Returns:
        pd.DataFrame: Dataframe with names cleaned
    """
    df_ticker["ticker_cleaned"] = df_ticker["ticker"].apply(lambda x: x.lower())
    clean_name_per_col(df_ticker, "name")
    clean_name_per_col(df_ticker, "full_name")

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


def select_relevant_cols(df_ticker: pd.DataFrame) -> pd.DataFrame:
    """Clean column names for ticker data

    Args:
        df_ticker (pd.DataFrame): Ticker Data

    Returns:
        pd.DataFrame: Subset of ticker data with cleaned column names
    """
    df_ticker = df_ticker[["Name", "Full Name", "Ticker_list"]]
    df_ticker = df_ticker.rename(columns={"Name": "name",
                                          "Full Name": "full_name",
                                          "Ticker_list": "ticker"})
    df_ticker = df_ticker.dropna()

    return df_ticker
