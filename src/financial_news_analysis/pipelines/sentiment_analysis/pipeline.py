"""
This is a boilerplate pipeline 'sentiment_analysis'
generated using Kedro 0.18.5
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import get_sentiment_from_df, combine_data_frames


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=get_sentiment_from_df,
            inputs=["title_sentences_all_the_news_data",
                    "params:column_name_to_analyze",
                    "params:model_name_general"],
            outputs="title_sentiments_all_the_news_data_model_general",
            name="get_general_sentiment_from_article_title"
        ),
        node(
            func=get_sentiment_from_df,
            inputs=["title_sentences_all_the_news_data",
                    "params:column_name_to_analyze",
                    "params:model_name_finance"],
            outputs="title_sentiments_all_the_news_data_model_finance",
            name="get_finance_sentiment_from_article_title"
        ),
        node(
            func=combine_data_frames,
            inputs=["title_sentiments_all_the_news_data_model_general",
                    "title_sentiments_all_the_news_data_model_finance"],
            outputs="title_sentiments_all_the_news_data",
            name="combine_sentiments_from_all_the_news_data"
        )
    ])
