from kedro.pipeline import *
from kedro.io import *
from kedro.runner import *

import pandas as pd


def pre_process_titels(df_titles: pd.DataFrame) -> pd.DataFrame:
    df_titles = df_titles.drop("Unnamed: 0", axis=1)
    df_titles = df_titles.dropna()
    df_titles = df_titles.rename(columns={"date": "date_time"}, errors="raise")
    df_titles["date"] = df_titles["date_time"].apply(
        lambda x: x.split(" ")[0]
    )

    return df_titles
