"""
This is a boilerplate pipeline 'match_deeplearning_ner_annoations'
generated using Kedro 0.18.5
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import ner_ticker_matching, filter_ner_annotations, clean_ner_annotations


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=filter_ner_annotations,
                inputs=["title_ner_annotations_all_the_news"],
                outputs="df_ner_filtered",
                name="filter_ner_annotations",
            ),
            node(
                func=clean_ner_annotations,
                inputs=["df_ner_filtered"],
                outputs="df_ner_cleaned",
                name="clean_ner_annotations"
            ),
            node(
                func=ner_ticker_matching,
                inputs=["df_ner_cleaned", "eikon_ticker_merged"],
                outputs="ner_ticker_matches",
                name="ner_ticker_matching",
            )
        ]
    )
