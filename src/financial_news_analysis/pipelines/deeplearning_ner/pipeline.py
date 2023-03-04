"""
This is a boilerplate pipeline 'deeplearning_ner'
generated using Kedro 0.18.5
"""


from kedro.pipeline import Pipeline, node, pipeline
from .nodes import create_sentence_df, create_ner_tags


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=create_sentence_df,
                inputs=["cleaned_with_text_col_all_the_news_data"],
                outputs="title_sentences_all_the_news_data",
                name="create_title_sentence_df"
            ),
            node(
                func=create_ner_tags,
                inputs=["title_sentences_all_the_news_data"],
                outputs="title_ner_annotations_all_the_news",
                name="ner_title_annotations"
            )
        ]
    )
