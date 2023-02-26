import string


def preprocess_string(text: str) -> str:
    """Clean string from special charactrs and upper case

    Args:
        text (str): raw text

    Returns:
        str: clean text
    """
    text = text.lower()
    text = text.translate(str.maketrans("", "", string.punctuation))

    return text
