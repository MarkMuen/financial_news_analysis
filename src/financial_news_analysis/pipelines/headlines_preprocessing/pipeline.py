"""
This is a boilerplate pipeline 'headlines_preprocessing'
generated using Kedro 0.18.5
"""
from kedro.pipeline.modular_pipeline import pipeline
from kedro.pipeline import Pipeline, node
from .nodes import preprocess_cnbc_headlines, preprocess_guardian_headlines, \
    preprocess_reuters_headlines, clean_month_desc, merge_headline_dfs, \
    clean_headline_texts, add_source_as_col


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=clean_month_desc,
                inputs=["cnbc_headlines", "params:headline_config_date_col"],
                outputs="cnbc_headlines_cleanded",
                name="cnbc_headline_cleaning"
            ),
            node(
                func=preprocess_cnbc_headlines,
                inputs=["cnbc_headlines_cleanded"],
                outputs="cnbc_headlines_preprocessed",
                name="cnbc_headline_preprocessing"
            ),
            node(
                func=add_source_as_col,
                inputs=["cnbc_headlines_preprocessed",
                        "params:headline_config_source_c"],
                outputs="cnbc_headlines_with_source",
                name="add_data_source_c"
            ),
            node(
                func=preprocess_guardian_headlines,
                inputs=["guardian_headlines"],
                outputs="guardian_headlines_preprocessed",
                name="guardian_headlines_preprocessing"
            ),
            node(
                func=add_source_as_col,
                inputs=["guardian_headlines_preprocessed",
                        "params:headline_config_source_g"],
                outputs="guardian_headlines_with_source",
                name="add_data_source_g"
            ),
            node(
                func=preprocess_reuters_headlines,
                inputs=["reuters_headlines"],
                outputs="reuters_headlines_preprocessed",
                name="reuters_headlines_preprocessing"
            ),
            node(
                func=add_source_as_col,
                inputs=["reuters_headlines_preprocessed",
                        "params:headline_config_source_r"],
                outputs="reuters_headlines_with_source",
                name="add_data_source_r"
            ),
            node(
                func=merge_headline_dfs,
                inputs=["cnbc_headlines_with_source",
                        "guardian_headlines_with_source",
                        "reuters_headlines_with_source"],
                outputs="merged_header_data",
                name="merge_data"
            ),
            node(
                func=clean_headline_texts,
                inputs=["merged_header_data"],
                outputs="cleaned_headline_data",
                name="clean_preprocessed_headlines"
            )
        ]
    )
