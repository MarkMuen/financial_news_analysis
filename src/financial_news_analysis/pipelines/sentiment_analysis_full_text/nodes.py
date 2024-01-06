from dataclasses import dataclass
import pandas as pd
from tqdm import tqdm
from .utils import create_text_classification_pipeline, TOKENIZER_KWARGS


@dataclass
class Sentiment():
    """Dataclass for sentiment
    """
    article_id: int
    source: str
    sentiment: str
    confidence: float


def select_relevant_columns(df_news: pd.DataFrame) -> pd.DataFrame:
    """Filter All the news Data to relevant columns
    for all the news sentiment analysis

    Args:
        df_news (pd.DataFrame): All the news data

    Returns:
        pd.DataFrame: All the news with relevant columns
    """
    return df_news[["text"]]


def random_sample_news_data(df_news: pd.DataFrame, sample_size: float) -> pd.DataFrame:
    """Random sample news data from the complete data set.
    Distinguish between abolut number and percentage to be sampled.

    Args:
        df_news (pd.DataFrame): All the news data set
        sample_size (float): Percentage or number of sampled items

    Returns:
        pd.DataFrame: Random sample of news data
    """
    if sample_size > 1:
        sample_size = int(sample_size)
        df_news_sample = df_news.sample(n=sample_size, axis=0, random_state=2)
    else:
        df_news_sample = df_news.sample(frac=sample_size, axis=0, random_state=2)

    return df_news_sample


def get_sentiment_for_article(df_news: pd.DataFrame,
                              model_name: str):

    pipe = create_text_classification_pipeline(model_name)
    data = []
    for row in tqdm(df_news.iterrows(), total=df_news.shape[0]):
        sentiment = pipe(row[1]["text"], **TOKENIZER_KWARGS)
        for prediction in sentiment[0]:
            data.append(
                Sentiment(
                    article_id=row[0],
                    source=model_name,
                    sentiment=prediction["label"],
                    confidence=prediction["score"]
                )
            )
    return pd.DataFrame(data)


def filter_max_confidence_sentiment(df_sents: pd.DataFrame) -> pd.DataFrame:
    """_summary_

    Args:
        df_sents (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    df_sents_filtered = df_sents.sort_values(["confidence"], ascending=True) \
        .groupby(["article_id", "source"]) \
        .tail(1)
    return df_sents_filtered
