from kedro.pipeline import Pipeline, node, pipeline
from .nodes import create_sentence_df, create_ner_tags, select_relevant_columns, \
    random_sample_news_data


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=select_relevant_columns,
                inputs=["cleaned_with_text_col_all_the_news_data"],
                outputs="the_news_data_relevant_cols_for_full_text_ner",
                name="select_relevant_columns_for_full_text_ner"
            ),
            node(
                func=random_sample_news_data,
                inputs=["the_news_data_relevant_cols_for_full_text_ner",
                        "params:number_of_samples_ner"],
                outputs="all_the_news_random_sample_for_full_text_ner",
                name="select_random_sample_for_full_text_ner"
            ),
            node(
                func=create_sentence_df,
                inputs=["all_the_news_random_sample_for_full_text_ner"],
                outputs="article_sentences_all_the_news_data_sample_full_text",
                name="create_article_sentence_df_full_text_ner"
            ),
            node(
                func=create_ner_tags,
                inputs=["article_sentences_all_the_news_data_sample_full_text"],
                outputs="article_ner_annotations_all_the_news_sample_full_text",
                name="ner_article_annotations_full_text_ner"
            )
        ]
    )
