"""
This is a boilerplate pipeline 'sentiment_analysis_full_text'
generated using Kedro 0.18.5
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import select_relevant_columns, random_sample_news_data, \
    get_sentiment_for_article, filter_max_confidence_sentiment, \
    compare_sentiment_overlap, compare_sentiment_correlation, \
    merge_comparison_results, create_sentiment_statistics, \
    combine_data_frames


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
                    "params:number_of_samples_sentiment"],
            outputs="all_the_news_random_sample",
            name="select_random_sample"
        ),
        node(
            func=get_sentiment_for_article,
            inputs=["all_the_news_random_sample",
                    "params:model_name_finance"],
            outputs="all_the_news_full_text_all_sentiments_sample_finance",
            name="run_sentiment_analysis_finance"
        ),
        node(
            func=get_sentiment_for_article,
            inputs=["all_the_news_random_sample",
                    "params:model_name_general"],
            outputs="all_the_news_full_text_all_sentiments_sample_general",
            name="run_sentiment_analysis_general"
        ),
        node(
            func=filter_max_confidence_sentiment,
            inputs=["all_the_news_full_text_all_sentiments_sample_finance"],
            outputs="all_the_news_full_text_sentiments_sample_finance",
            name="select_maximum_sentiment_finance"
        ),
        node(
            func=filter_max_confidence_sentiment,
            inputs=["all_the_news_full_text_all_sentiments_sample_general"],
            outputs="all_the_news_full_text_sentiments_sample_general",
            name="select_maximum_sentiment_general"
        ),
        node(
            func=compare_sentiment_overlap,
            inputs=["title_sentiments_all_the_news_data_filtered_extended",
                    "all_the_news_full_text_sentiments_sample_finance",
                    "params:model_name_finance",
                    "params:number_of_samples_sentiment"],
            outputs="all_the_news_sentiments_overlap_finance",
            name="compare_sentiment_overlap_finance"
        ),
        node(
            func=compare_sentiment_overlap,
            inputs=["title_sentiments_all_the_news_data_filtered_extended",
                    "all_the_news_full_text_sentiments_sample_general",
                    "params:model_name_general",
                    "params:number_of_samples_sentiment"],
            outputs="all_the_news_sentiments_overlap_general",
            name="compare_sentiment_overlap_general"
        ),
        node(
            func=compare_sentiment_correlation,
            inputs=["title_sentiments_all_the_news_data_filtered_extended",
                    "all_the_news_full_text_sentiments_sample_finance",
                    "params:model_name_finance",
                    "params:number_of_samples_sentiment"],
            outputs="all_the_news_sentiments_coorelation_finance",
            name="compare_sentiment_correlation_fiance"
        ),
        node(
            func=compare_sentiment_correlation,
            inputs=["title_sentiments_all_the_news_data_filtered_extended",
                    "all_the_news_full_text_sentiments_sample_general",
                    "params:model_name_general",
                    "params:number_of_samples_sentiment"],
            outputs="all_the_news_sentiments_coorelation_general",
            name="compare_sentiment_correlation_general"
        ),
        node(
            func=merge_comparison_results,
            inputs=["all_the_news_sentiments_overlap_finance",
                    "all_the_news_sentiments_coorelation_finance",
                    "all_the_news_sentiments_overlap_general",
                    "all_the_news_sentiments_coorelation_general"],
            outputs="all_the_news_sentiments_comparison",
            name="merge_comparison_results"
        ),
        node(
            func=combine_data_frames,
            inputs=["all_the_news_full_text_sentiments_sample_general",
                    "all_the_news_full_text_sentiments_sample_finance"],
            outputs="full_text_sentiments_all_the_news_data",
            name="merge_full_text_sentimens"
        ),
        node(
            func=create_sentiment_statistics,
            inputs=["full_text_sentiments_all_the_news_data"],
            outputs="all_the_news_full_text_sentiments_stats",
            name="create_sentiment_statistics_full_text"
        )
    ])
