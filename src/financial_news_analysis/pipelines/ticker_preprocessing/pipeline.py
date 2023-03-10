from kedro.pipeline import Pipeline, node, pipeline
from .nodes import select_relevant_cols, clean_names_in_df, search_alternative_name, \
    select_relevant_cols_rh, join_ticker_data_sources


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
            [
                node(
                    func=select_relevant_cols,
                    inputs=["eikon_ticker"],
                    outputs="eikon_ticker_subset",
                    name="select_relevant_cols"
                ),
                node(
                    func=clean_names_in_df,
                    inputs=["eikon_ticker_subset"],
                    outputs="eikon_ticker_cleaned",
                    name="clean_ticker_names"
                ),
                node(
                    func=search_alternative_name,
                    inputs=["eikon_ticker_cleaned"],
                    outputs="eikon_ticker_preprocessed",
                    name="search_alternative_names"
                ),
                node(
                    func=select_relevant_cols_rh,
                    inputs=["robin_hood_ticker"],
                    outputs="robin_hood_ticker_preprocessed",
                    name="preprocess_rh_ticker"
                ),
                node(
                    func=join_ticker_data_sources,
                    inputs=["eikon_ticker_preprocessed", 
                            "robin_hood_ticker_preprocessed"],
                    outputs="ek_rh_merged_ticker_data",
                    name="merge_ticker_data"
                )
            ]
        )
