"""
This is a boilerplate pipeline 'match_deeplearning_ner_annoations'
generated using Kedro 0.18.5
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import ner_ticker_matching, filter_ner_annotations, \
    clean_ner_annotations, merge_ner_anntotations


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=merge_ner_anntotations,
                inputs=["title_ner_annotations_all_the_news",
                        "article_ner_annotations_all_the_news"],
                outputs="merged_ner_annotations_all_the_news",
                name="merge_ner_anntotations",
            ),
            node(
                func=filter_ner_annotations,
                inputs=["merged_ner_annotations_all_the_news"],
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
            ),
            node(
                func=filter_ner_annotations,
                inputs=["article_ner_annotations_all_the_news_sample_full_text"],
                outputs="df_ner_filtered_full_text_sample",
                name="filter_ner_annotations_full_text",
            ),
            node(
                func=clean_ner_annotations,
                inputs=["df_ner_filtered_full_text_sample"],
                outputs="df_ner_cleaned_full_text_sample",
                name="clean_ner_annotations_full_text"
            ),
            node(
                func=ner_ticker_matching,
                inputs=["df_ner_cleaned_full_text_sample", "eikon_ticker_merged"],
                outputs="ner_ticker_matches_full_text_sample",
                name="ner_ticker_matching_full_text",
            )
        ]
    )
