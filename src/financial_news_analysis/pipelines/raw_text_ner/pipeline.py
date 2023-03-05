"""
This is a boilerplate pipeline 'raw_text_ner'
generated using Kedro 0.18.5
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import create_annotations


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
            [
                node(
                    func=create_annotations,
                    inputs=["cleaned_with_text_col_all_the_news_data",
                            "eikon_ticker_preprocessed"],
                    outputs="article_raw_ner_annotations_all_the_news",
                    name="make_raw_ner_annotations"
                )
            ]
        )
