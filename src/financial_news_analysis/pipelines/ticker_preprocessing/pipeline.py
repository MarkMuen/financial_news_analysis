from kedro.pipeline import Pipeline, node, pipeline
from .nodes import select_relevant_cols_ek_ticker, clean_names_in_df, \
    search_alternative_name, select_relevant_names_ek_names, merge_ek_data, \
    select_relevant_cols_rh, join_ticker_data_sources, preprocess_ek_names_data


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
            [
                node(
                    func=select_relevant_cols_ek_ticker,
                    inputs=["eikon_ticker"],
                    outputs="eikon_ticker_subset",
                    name="select_relevant_cols_ek_ticker"
                ),
                node(
                    func=clean_names_in_df,
                    inputs=["eikon_ticker_subset",
                            "params:name_columns_eikon_ticker"],
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
                    func=select_relevant_names_ek_names,
                    inputs=["eikon_names"],
                    outputs="eikon_names_subset",
                    name="select_cols_eikon_names"
                ),
                node(
                    func=preprocess_ek_names_data,
                    inputs=["eikon_names_subset"],
                    outputs="eikon_names_preprocessed",
                    name="preprocess_eikon_names"
                ),
                node(
                    func=clean_names_in_df,
                    inputs=["eikon_names_preprocessed",
                            "params:name_columns_eikon_names"],
                    outputs="eikon_names_cleaned",
                    name="clean_names_eikon_names"
                ),
                node(
                    func=select_relevant_cols_rh,
                    inputs=["robin_hood_ticker"],
                    outputs="robin_hood_ticker_preprocessed",
                    name="preprocess_rh_ticker"
                ),
                node(
                    func=merge_ek_data,
                    inputs=["eikon_ticker_preprocessed",
                            "eikon_names_cleaned"],
                    outputs="eikon_ticker_merged",
                    name="merge_eikon_data"
                ),
                node(
                    func=join_ticker_data_sources,
                    inputs=["eikon_ticker_merged",
                            "robin_hood_ticker_preprocessed"],
                    outputs="ek_rh_merged_ticker_data",
                    name="merge_ticker_data"
                )
            ]
        )
