import string 
from dataclasses import dataclass
from typing import Callable
import pandas as pd
from nltk.tokenize import word_tokenize
from cleanco import basename
from tqdm import tqdm

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


@dataclass
class NerMatch():
    """Dataclass for NER matches with ticker data
    """
    ner_id: int
    ticker_name: str


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


def match_strict(ner: str, 
                 name: str,
                 f_eval_tokens: Callable[[], bool],
                 f_eval_names: Callable[[], bool]) -> bool:
    """Match NER and ticker name with strict criteria,
       meaning that all tokens in the NER and ticker name must match

    Args:
        ner (str): _description_
        name (str): _description_
        f_eval_tokens (Callable): _description_
        f_eval_names (Callable): _description_

    Returns:
        bool: _description_
    """
    if ner and name:
        tokens_ner = ner.split(" ")
        tokens_name = name.split(" ")
        bools_1 = [f_eval_tokens([t_ner == t_name for t_name in tokens_name])
                   for t_ner in tokens_ner]
        bools_2 = [f_eval_tokens([t_ner == t_name for t_ner in tokens_ner])
                   for t_name in tokens_name]
        return f_eval_names(bools_1) and f_eval_names(bools_2)
    else:
        return False


def combine_name_matches(df_matches):
    """_summary_

    Args:
        df_matches (_type_): _description_

    Returns:
        _type_: _description_
    """
    df_matches = df_matches.sum(axis=1)
    df_matches = df_matches.apply(lambda x: True if x >= 1 else False)
    return df_matches


def calculate_matches(ner: str, df_ticker: pd.DataFrame) -> pd.DataFrame:
    """
    Args:
        ner (str): _description_
        df_ticker (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    df_matches = pd.DataFrame()
    name_cols = [col for col in df_ticker.columns if ("name" in col)
                 and (col.endswith("_cleaned"))]

    for col in name_cols:
        df_matches[col] = df_ticker[col].apply(lambda x: match_strict(ner, x, all, all))

    return df_matches


def set_column_names(df_matches: pd.DataFrame, df_ticker: pd.DataFrame) -> pd.DataFrame:
    """
    Args:
        df_matches (pd.DataFrame): _description_
        df_ticker (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_"""

    df_matches.columns = df_ticker["ticker_cleaned"].tolist()
    return df_matches


def format_match_data(df_matches: pd.DataFrame) -> pd.DataFrame:
    """_summary_

    Args:
        df_matches (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    data = []
    for col in tqdm(df_matches.columns):
        for anno in df_matches[df_matches[col]].index.tolist():
            data.append(NerMatch(anno, col))

    return pd.DataFrame(data)
