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
        print("Sample size is larger than 1, using absolute number of samples")
        sample_size = int(sample_size)
        df_news_sample = df_news.sample(n=sample_size, axis=0, random_state=2)
    elif sample_size < 1:
        print("Sample size is smaller than 1, using percentage of samples")
        df_news_sample = df_news.sample(frac=sample_size, axis=0, random_state=2)
    else:
        print("Sample size is 1, using all the data")
        df_news_sample = df_news.copy()

    return df_news_sample


def get_sentiment_for_article(df_news: pd.DataFrame,
                              model_name: str) -> pd.DataFrame:
    """Create sentiment for each article using DL model and full text

    Args:
        df_news (pd.DataFrame): Pandas dataframe with news data and full texts
        model_name (str): Name of the model used for sentiment analysis

    Returns:
        pd.DataFrame: Sentiment predictions for each article and sentiment
    """
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
    """Filter sentiment with highest confidence

    Args:
        df_sents (pd.DataFrame): Sentiments created using DL model

    Returns:
        pd.DataFrame: Maximum confidence sentiment per article
    """
    df_sents_filtered = df_sents.sort_values(["confidence"], ascending=True) \
        .groupby(["article_id", "source"]) \
        .tail(1)
    return df_sents_filtered


def compare_sentiment_overlap(df_sents_title: pd.DataFrame,
                              df_sents_full_text: pd.DataFrame,
                              model_name: str,
                              sample_size: float) -> pd.DataFrame:
    """Compare sentiments created using title and full text

    Args:
        df_sents_title (pd.DataFrame): Sentiments created using title
        df_sents_full_text (pd.DataFrame): Sentiments created using full text
        model_name (str): Model name used for sentiment analysis
        sample_size (float): Percentage or number of sampled items

    Returns:
        pd.DataFrame: Comparison of overlapping sentiments
    """
    df_sents_title = df_sents_title[df_sents_title["source"] == model_name].copy()
    df_sents_full_text = df_sents_full_text[
            df_sents_full_text["source"] == model_name
        ].copy()
    df_sents_merged = df_sents_title.merge(df_sents_full_text,
                                           on="article_id",
                                           suffixes=("_title", "_full_text"))
    mask = df_sents_merged["sentiment_title"] == df_sents_merged["sentiment_full_text"]
    df_sents_merged["sentiment_match"] = mask
    accuracy = df_sents_merged["sentiment_match"].sum() / df_sents_merged.shape[0]
    sample_size = str(sample_size) if sample_size and int(sample_size) != 1 else "all"
    result = pd.DataFrame([{"model": model_name,
                            "sample_size": sample_size,
                            "measure": "accuracy",
                            "value": accuracy}])
    return result


def compare_sentiment_correlation(df_sents_title: pd.DataFrame,
                                  df_sents_full_text: pd.DataFrame,
                                  model_name: str,
                                  sample_size: float) -> pd.DataFrame:
    """Compare sentiments created using title and full text by calculating correlation
    of encoded sentiments

    Args:
        df_sents_title (pd.DataFrame): Sentiments created using title
        df_sents_full_text (pd.DataFrame): Sentiments created using full text
        model_name (str): Model name used for sentiment analysis
        sample_size (float): Percentage or number of sampled items

    Returns:
        pd.DataFrame: Correlation of encoded sentiments
    """

    df_sents_title = df_sents_title[df_sents_title["source"] == model_name].copy()
    df_sents_full_text = df_sents_full_text[
        df_sents_full_text["source"] == model_name].copy()

    df_sents_title["sentiment_encoded"] = df_sents_title["sentiment"].map({
        "positive": 1,
        "negative": -1,
        "neutral": 0
    })
    df_sents_full_text["sentiment_encoded"] = df_sents_full_text["sentiment"].map({
        "positive": 1,
        "negative": -1,
        "neutral": 0
    })
    df_sents_merged = df_sents_title.merge(df_sents_full_text,
                                           on="article_id",
                                           suffixes=("_title", "_full_text"))
    correlation = df_sents_merged["sentiment_encoded_title"].corr(
        df_sents_merged["sentiment_encoded_full_text"]
    )
    sample_size = str(sample_size) if sample_size and int(sample_size) != 1 else "all"
    result = pd.DataFrame([{"model": model_name,
                            "sample_size": sample_size,
                            "measure": "correlation",
                            "value": correlation}])
    return result


def merge_comparison_results(df_overlap_finance: pd.DataFrame,
                             df_correlation_finance: pd.DataFrame,
                             df_overlap_general: pd.DataFrame,
                             df_correlation_general: pd.DataFrame) -> pd.DataFrame:
    """Merge comparison results

    Args:
        df_overlap_finance (pd.DataFrame): Comparison of overlapping sentiments
            for finance model
        df_correlation_finance (pd.DataFrame): Correlation of encoded sentiments
            for finance model
        df_overlap_general (pd.DataFrame): Comparison of overlapping sentiments
            for general model
        df_correlation_general (pd.DataFrame): Correlation of encoded sentiments
            for general model
    Returns:
        pd.DataFrame: Comparison results
    """
    result = pd.concat([df_overlap_finance,
                        df_correlation_finance,
                        df_overlap_general,
                        df_correlation_general])
    return result


def create_sentiment_statistics(df_sents: pd.DataFrame) -> pd.DataFrame:
    """_summary_

    Args:
        df_sents (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    stats = df_sents.groupby(["source", "sentiment"])["sen_id"]\
        .count().reset_index(name="count")
    return stats


def combine_data_frames(df_general: pd.DataFrame,
                        df_finance: pd.DataFrame) -> pd.DataFrame:
    """_summary_

    Args:
        df_general (pd.DataFrame): _description_
        df_finance (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    return pd.concat([df_general, df_finance])
