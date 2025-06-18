from pandas import DataFrame, ExcelWriter


def save_df_2_excel(
    df: DataFrame, file_path: str, sheet_name: str = 'List1'
):
    try:
        with ExcelWriter(file_path, mode='a', if_sheet_exists='replace') as wr:
            df.to_excel(wr, sheet_name=sheet_name, index=False)

    except FileNotFoundError:
        with ExcelWriter(file_path, mode='w') as wr:
            df.to_excel(wr, sheet_name=sheet_name, index=False)
