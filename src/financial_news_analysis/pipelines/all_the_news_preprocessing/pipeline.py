"""
This is a boilerplate pipeline 'all_the_news_preprocessing'
generated using Kedro 0.18.5
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import preprocess_data, clean_texts, filter_news_data


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=preprocess_data,
                inputs=["all_the_news"],
                outputs="preprocessed_news",
                name="all_the_news_prepocessed"
            ),
            node(
                func=clean_texts,
                inputs=["preprocessed_news"],
                outputs="cleaned_news",
                name="clean_news_texts"
            ),
            node(
                func=filter_news_data,
                inputs=["cleaned_news", "params:start_date"],
                outputs="cleaned_all_the_news_data",
                name="filter_data"
            )
        ]
    )
