import logging
from dataclasses import dataclass, field
from typing import List
import pandas as pd
from tqdm import tqdm

tqdm.pandas()


@dataclass
class Ner_Annotation:
    """Dataclass that stores information for company name matches"""
    article_id: int
    ticker_id: str
    match_index: int
    text_length: int
    percentage_pos: float = field(init=False)

    def __post_init__(self):
        self.percentage_pos = round(float(self.match_index/self.text_length), 6)


def create_annotations_from_raw_text(art_idx: int,
                                     news: str,
                                     ticker: str,
                                     names: List[str]) -> Ner_Annotation:
    """ Function to find company names in preprocessed news article 

    Args:
        art_idx (int): Index of current article
        news (str): Preprocessed text of article
        ticker (str): Name of the ticker
        names (List[str]): List of relevant company names

    Returns:
        Ner_Annotation: Found match for this company
    """
    anno = None
    idx = [news.find(n) for n in names if n is not None]

    if any([i > -1 for i in idx]):
        idx = [i for i in idx if i > -1]
        anno = Ner_Annotation(
            article_id=art_idx,
            ticker_id=ticker,
            match_index=min(idx),
            text_length=len(news)
        )
    return anno


def create_annotations(df_news: pd.DataFrame,
                       df_ticker: pd.DataFrame) -> pd.DataFrame:
    """Function to search company names from ticker data set in news articles data set

    Args:
        df_news (pd.DataFrame): Data set of news data with article texts
        df_ticker (pd.DataFrame): Data set of tickers to search for in news texts

    Returns:
        pd.DataFrame: Matches found in the articles
    """
    data = []
    for i, r in df_ticker.iterrows():
        logging.info(f"processing ticker {i}")

        ticker = r['ticker']
        name_short = r['name_cleaned']
        name_long = r['full_name_cleaned']
        name_alt = r['alternative_name']

        annos = df_news.progress_apply(
            lambda row: create_annotations_from_raw_text(
                row.name,
                row['text_cleaned'],
                ticker,
                [name_short, name_long, name_alt]
            ),
            axis=1
        )
        annos = annos.dropna()
        data.extend(annos.to_list())

    return pd.DataFrame(data)
