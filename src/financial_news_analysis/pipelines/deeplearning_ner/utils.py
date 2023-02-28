import nltk
from typing import List, Dict, Any
import pandas as pd
import flair
from flair.models import SequenceTagger

TAGGER = SequenceTagger.load("flair/ner-english-ontonotes-large")


def split_into_sentences(text: str) -> List[str]:
    """Split text into sentences

    Args:
        text (str): raw text

    Returns:
        List[str]: List of sentences
    """
    return nltk.tokenize.sent_tokenize(text, language='english')


def process_article_to_sents(row: pd.Series) -> pd.DataFrame:
    """Create a dataframe with sentences from text in pandas row

    Args:
        row (pd.Series): row of raw data set

    Returns:
        pd.DataFrame: dataframe of sentences and articel id
    """
    sents = split_into_sentences(row["text"])
    data = [[row.name, i, s] for i, s in enumerate(sents)]
    df_sents = pd.DataFrame(data, columns=["article_id", "sentence_nr", "sentence"])
    return df_sents


def create_annotation_data_set(anno_data: List[Dict[str, Any]],
                               doc_id: int,
                               sen_id: int,
                               sen_num: int,
                               ) -> pd.DataFrame:
    """Create Dataset from annotations

    Args:
        anno_data (List[Dict[str, Any]]): NER Annotations
        doc_id (int): Index of corresponding article
        sen_id (int): Index of corresponding sentence
        sen_num (int): Number of sentence in article

    Returns:
        pd.DataFrame: _description_
    """

    for anno in anno_data:
        anno["article_id"] = doc_id
        anno["sentence_id"] = sen_id
        anno["sentence_nr"] = sen_num

    return pd.DataFrame(anno_data)


def get_ner_annotations(text: str,
                        tagger: flair.models.SequenceTagger) -> flair.data.Sentence:
    """Perform NER prediciton with FLAIR

    Args:
        text (str): Raw Text
        tagger (flair.models.SequenceTagger): Flair model for tagging

    Returns:
        flair.data.Sentence: Flair sentence object with annotations
    """
    sentence = flair.data.Sentence(text)
    tagger.predict(sentence)
    return sentence


def parse_ner_annotation(sentence: flair.data.Sentence) -> List[Dict[str, Any]]:
    """Transorm annotations in a list of relevant attributes

    Args:
        sentence (flair.data.Sentence): Sentence object with NER annotations

    Returns:
        List[List[str]]: list of annotation attributes
    """
    annos = []
    for entity in sentence.get_spans("ner"):
        annos.append(
            {
                "text": entity.text,
                "label": entity.get_label("ner").value,
                "confidence": entity.get_label("ner").score,
                "start_pos": entity.start_position,
                "end_pos": entity.end_position
            }
        )
    return annos


def perform_ner_annotation(row: pd.Series) -> pd.DataFrame:

    sentence = get_ner_annotations(row["sentence"], TAGGER)
    sentence_annos = parse_ner_annotation(sentence)
    sentence_annos = create_annotation_data_set(sentence_annos,
                                                row["article_id"],
                                                row.name,
                                                row["sentence_nr"])
    return sentence_annos
