"""
This is a boilerplate pipeline 'create_reporting'
generated using Kedro 0.18.5
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import add_article_data, drop_id_columns, clean_data_types


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=add_article_data,
            inputs=["ner_ticker_matches",
                    "ek_rh_merged_ticker_data",
                    "title_ner_annotations_all_the_news",
                    "all_the_news"],
            outputs="df_matched_data",
            name="add_article_data"
        ),
        node(
            func=drop_id_columns,
            inputs="df_matched_data",
            outputs="df_matched_data_columns_dropped",
            name="drop_id_columns"
        ),
        node(
            func=clean_data_types,
            inputs="df_matched_data_columns_dropped",
            outputs="df_matched_data_cleaned",
            name="clean_data_types"
        )
    ])
