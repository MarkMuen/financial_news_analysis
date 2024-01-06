"""
This is a boilerplate pipeline 'sentiment_analysis'
generated using Kedro 0.18.5
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import get_sentiment_from_df, combine_data_frames, \
    filter_max_confidence_sentiment, create_sentiment_statistics, add_article_data


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
        ),
        node(
            func=filter_max_confidence_sentiment,
            inputs=["title_sentiments_all_the_news_data"],
            outputs="title_sentiments_all_the_news_data_filtered",
            name="filter_sentiments_with_highest_confidence"
        ),
        node(
            func=add_article_data,
            inputs=["title_sentiments_all_the_news_data_filtered",
                    "title_sentences_all_the_news_data"],
            outputs="title_sentiments_all_the_news_data_filtered_extended",
            name="add_article_data_to_sentiments"
        ),
        node(
            func=create_sentiment_statistics,
            inputs=["title_sentiments_all_the_news_data_filtered"],
            outputs="stats_sentiments_all_the_news_data_filtered",
            name="create_stats_for_sentiments"
        )
    ])
