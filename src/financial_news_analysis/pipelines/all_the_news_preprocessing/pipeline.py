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
                outputs="preprocessed_all_the_news",
                name="preprocess_news"
            ),
            node(
                func=filter_news_data,
                inputs=["preprocessed_all_the_news", "params:start_date"],
                outputs="filtered_all_the_news_data",
                name="filer_news"
            ),
            node(
                func=clean_texts,
                inputs=["filtered_all_the_news_data"],
                outputs="cleaned_all_the_news_data",
                name="clean_news"
            )
        ]
    )
