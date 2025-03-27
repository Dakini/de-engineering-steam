import pandas as pd
import ast


def drop_columns(df, columns):

    return df.drop(columns)


def process_null(df):
    convert_to_none = [
        "",
        " ",
        "None",
        "none",
        "null",
        "N/a",
        "n/a",
        "N/A",
        "NA",
        '["none"]',
        '["null"]',
        "{}",
    ]
    df.replace(convert_to_none, None, inplace=True)
    return df


# def return_list_as_string(val):

#     try:
#         val_list = ast.literal_eval(val)
#         if isinstance(val_list, list):
#             return ",".join(filter(None, val_list))
#     except (ValueError, SyntaxError):
#         return ""
#     return val


def clean_steam_spy(df):
    cols_to_drop = [
        "score_rank",  # seems to have no values
        "user_score",  # user score is all zeros
        "",
    ]
