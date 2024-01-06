"""
This is a boilerplate pipeline 'sentiment_analysis_full_text'
generated using Kedro 0.18.5
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import select_relevant_columns, random_sample_news_data, \
    get_sentiment_for_article, filter_max_confidence_sentiment, \
    compare_sentiment_overlap, compare_sentiment_correlation, merge_comparison_results


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=select_relevant_columns,
            inputs=["cleaned_with_text_col_all_the_news_data"],
            outputs="the_news_data_relevant_cols",
            name="select_relevant_columns"
        ),
        node(
            func=random_sample_news_data,
            inputs=["the_news_data_relevant_cols",
                    "params:number_of_samples"],
            outputs="all_the_news_random_sample",
            name="select_random_sample"
        ),
        node(
            func=get_sentiment_for_article,
            inputs=["all_the_news_random_sample",
                    "params:model_name_finance"],
            outputs="all_the_news_full_text_all_sentiments_sample",
            name="run_sentiment_analysis"
        ),
        node(
            func=filter_max_confidence_sentiment,
            inputs=["all_the_news_full_text_all_sentiments_sample"],
            outputs="all_the_news_full_text_sentiments_sample",
            name="select_maximum_sentiment"
        ),
        node(
            func=compare_sentiment_overlap,
            inputs=["title_sentiments_all_the_news_data_filtered_extended",
                    "all_the_news_full_text_sentiments_sample",
                    "params:number_of_samples"],
            outputs="all_the_news_sentiments_overlap",
            name="compare_sentiment_overlap"
        ),
        node(
            func=compare_sentiment_correlation,
            inputs=["title_sentiments_all_the_news_data_filtered_extended",
                    "all_the_news_full_text_sentiments_sample",
                    "params:number_of_samples"],
            outputs="all_the_news_sentiments_coorelation",
            name="compare_sentiment_correlation"
        ),
        node(
            func=merge_comparison_results,
            inputs=["all_the_news_sentiments_overlap",
                    "all_the_news_sentiments_coorelation"],
            outputs="all_the_news_sentiments_comparison",
            name="merge_comparison_results"
        )
    ])
