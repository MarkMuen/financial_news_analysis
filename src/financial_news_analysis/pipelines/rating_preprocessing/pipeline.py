"""
This is a boilerplate pipeline 'rating_preprocessing'
generated using Kedro 0.18.5
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import pre_process_titels


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=pre_process_titels,
                inputs=["analyst_ratings_titles"],
                outputs="analyst_ratings_titles_preprocessed",
                name="preprocess_rating_titles"
            )
        ]
    )
