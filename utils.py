import time
from typing import List, Dict, Tuple, Type, Union, Optional

from datetime import datetime
import os
import platform

import numpy as np
import pandas as pd
from pandas.api.types import union_categoricals

_PLATFORM = platform.system()
print(f"Running on {_PLATFORM}")

#_DEBUG_BASE_DIRECTORY = r"C:\Users\khashkes\Documents\USAID_Credence\Projects\DDC\2021 Q4\Debug"
# _DEBUG_BASE_DIRECTORY = r"~/Debug"
# _DEBUG_BASE_DIRECTORY = r"s3://e1-devtest-ddc-gov-usaid/ddc-quarterly-analytics/output"
_DEBUG_WRITE_ENCODING = "utf-8-sig"

if _PLATFORM == 'Windows':
    _CSV_DATE_FORMAT = "%#m/%#d/%Y"
else:
    _CSV_DATE_FORMAT = "%-m/%-d/%Y"


ColumnDtypes = Dict[str, Union[Type, str]]


def fiscal_year_and_quarter_from_date(dt: datetime) -> Tuple[int, str]:
    fiscal_year = dt.year
    if dt.month == 10:
        quarter = "Q1"
        fiscal_year += 1
    elif dt.month == 1:
        quarter = "Q2"
    elif dt.month == 4:
        quarter = "Q3"
    elif dt.month == 7:
        quarter = "Q4"
    else:
        raise(ValueError(f"Invalid month [{dt.month}]"))
    return fiscal_year, quarter


def date_from_fiscal_year_and_quarter(fiscal_year: int, quarter: str) -> datetime:
    year = fiscal_year
    if quarter == "Q1":
        year -= 1
        month = 10
    elif quarter == "Q2":
        month = 1
    elif quarter == "Q3":
        month = 4
    elif quarter == "Q4":
        month = 7
    else:
        raise(ValueError(f"Invalid quarter [{quarter}]"))
    return datetime(year=year, month=month, day=1)


def fy_from_fiscal_year(fiscal_year: int) -> str:
    return f"FY{fiscal_year % 100:02d}"


def fiscal_year_from_fy(fy: str) -> int:
    return 2000 + int(fy[2:4])


def fyq_from_fy_and_quarter(fy: str, quarter: str) -> str:
    return f"{fy} {quarter}"


def fy_and_quarter_from_fyq(fyq: str) -> Tuple[str, str]:
    return fyq[:4], fyq[5:7]


def fyq_from_fiscal_year_and_quarter(fiscal_year: int, quarter: str) -> str:
    return fyq_from_fy_and_quarter(fy_from_fiscal_year(fiscal_year), quarter)


def fyq_from_date(dt: datetime) -> str:
    fiscal_year, quarter = fiscal_year_and_quarter_from_date(dt)
    return fyq_from_fiscal_year_and_quarter(fiscal_year, quarter)


def fiscal_year_and_quarter_from_fyq(fyq: str) -> Tuple[int, str]:
    fy, quarter = fy_and_quarter_from_fyq(fyq)
    return fiscal_year_from_fy(fy), quarter


def fiscal_year_from_fyq(fyq: str) -> int:
    return fiscal_year_and_quarter_from_fyq(fyq)[0]


def quarter_from_fyq(fyq: str) -> str:
    return fiscal_year_and_quarter_from_fyq(fyq)[1]


def fyq_compare(fyq1: str, fyq2: str) -> int:
    """Compare 2 FYQ values, returning -1, 0, +1 to indicate smaller, equal, larger."""
    fiscal_year1, quarter1 = fiscal_year_and_quarter_from_fyq(fyq1)
    fiscal_year2, quarter2 = fiscal_year_and_quarter_from_fyq(fyq2)
    if fiscal_year1 > fiscal_year2:
        return 1
    if fiscal_year1 < fiscal_year2:
        return -1
    if quarter1[0] == "Q" and quarter2[0] == "Q":
        if quarter1 > quarter2:
            return 1
        if quarter1 < quarter2:
            return -1
        return 0
    return 0


def date_from_fyq(fyq: str) -> datetime:
    if len(fyq) != 7 or fyq[:2] != "FY" or fyq[5] != "Q":
        return pd.NaT
    fiscal_year, quarter = fiscal_year_and_quarter_from_fyq(fyq)
    return date_from_fiscal_year_and_quarter(fiscal_year, quarter)


def move_column(df: pd.DataFrame, column: str, loc: int) -> pd.DataFrame:
    """Move a dataframe's column to a new location.
    Args:
        df (pd.DataFrame): dataframe to use
        column (str): column name to move
        loc (int): where to relocate column to
    Returns:
        pd.DataFrame: re-assigned dataframe
    """
    temp_col = df.pop(column)
    df.insert(loc=loc, column=column, value=temp_col)
    return df


def add_category(df: pd.DataFrame, column: str, category: str) -> None:
    """Add category to column if it is new"""
    if category not in df[column].cat.categories:
        df.loc[:, column] = df[column].cat.add_categories([category])


def add_categories(df: pd.DataFrame, column: str, categories: List[str]) -> None:
    """Add category to column if it is new"""
    for category in categories:
        add_category(df, column, category)


def set_category_column(df: pd.DataFrame,
                        column: str,
                        value: Optional[str] = None,
                        values: Optional[Union[list, np.ndarray, pd.Series]] = None,
                        value_map: Optional[Dict[str, str]] = None,
                        recalc_categories: bool = False
                        ) -> None:
    """Sets a category column to the provided values, keeping existing Categories if column already exists.

    Args:
        df (pd.DataFrame): Dataframe
        column (str): Column name to update
        value (Optional[str]): Single value used to update entire column
        values (Optional[Union[list, np.ndarray, pd.Series]]): Array of values used to update column
        value_map: Dict mapping used to update column values
        recalc_categories (bool): calculate categories based on new values or reuse existing categories
    """
    if column not in df.columns:
        recalc_categories = True

    if value is not None:
        values = [value] * len(df.index)
        if not recalc_categories:
            add_category(df=df, column=column, category=value)
    elif value_map is not None:
        values = df[column].map(value_map).fillna(df[column])
        if not recalc_categories:
            add_categories(df=df, column=column, categories=[value for value in value_map.values()])

    if recalc_categories:
        column_dtype = 'category'
    else:
        column_dtype = df[column].dtype

    df.loc[:, column] = pd.Series(values, index=df.index, dtype=column_dtype)


def unify_categories(dfs: List[pd.DataFrame],
                     column: Optional[str] = None,
                     columns: Optional[List[str]] = None
                     ) -> None:
    """Converts column categories to unified category.

    Args:
        dfs (List[pd.DataFrame]): List of dataframes over which to unify column categories
        column (Optional[str]): Single column name shared by all dfs
        columns (Optional[List[str]]): Different column names per df - len(dfs) == len(columns)
    """
    if column is not None:
        columns = [column] * len(dfs)

    # Generate the union category across dfs for this column
    uc = union_categoricals([df[column] for df, column in zip(dfs, columns)])

    # Change to union category for all dataframes
    for df, column in zip(dfs, columns):
        df.loc[:, column] = pd.Categorical(df[column].values, categories=uc.categories)


def concatenate_dfs(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate while preserving categorical columns.

    Note: Column categories are changed inplace for the input dataframes

    Args:
        dfs (List[pd.DataFrame]): List of dataframes to concatenate

    Returns:
        pd.DataFrame: Concatenated dataframes with updated categories
    """
    # Iterate on categorical columns common to all dfs
    columns = set.intersection(*[set(df.columns[df.dtypes.values == 'category']) for df in dfs])
    for column in columns:
        unify_categories(dfs, column)
    return pd.concat(dfs, ignore_index=True)


def sort_text_file(base_directory: str, input_file_relative_path: str, output_file_relative_path: str):
    """Sort text file.

    Args:
        base_directory (str): Base path for input and output files
        input_file_relative_path (str): Input file
        output_file_relative_path (str): Output file
    """
    if _PLATFORM != 'Windows':
        return
    input_file_relative_path = os.path.join(base_directory, input_file_relative_path)
    output_file_relative_path = os.path.join(base_directory, output_file_relative_path)
    ret_code = os.system(f'cmd /c sort "{input_file_relative_path}" /o "{output_file_relative_path}"')
    if ret_code != 0:
        print(f'Failed to sort file {input_file_relative_path}')


def compare_text_files(base_directory: str, file1_relative_path: str, file2_relative_path: str):
    """Compare 2 text files

    Args:
        base_directory (str): Base directory for compared files
        file1_relative_path (str): First file to compare
        file2_relative_path (str): Second file to compare
    """
    if _PLATFORM != 'Windows':
        return
    file1_relative_path = os.path.join(base_directory, file1_relative_path)
    file2_relative_path = os.path.join(base_directory, file2_relative_path)
    ret_code = os.system(f'cmd /c fc /N /T "{file1_relative_path}" "{file2_relative_path}"')
    if ret_code != 0:
        print('FC: files are NOT the same!')


def read_csv(base_directory: str,
             file_relative_path: str,
             encoding: str,
             na_values: List[str],
             column_dtypes: ColumnDtypes
             ) -> pd.DataFrame:
    """Generic CSV Reader."""
    # Create converters to strip whitespace when reading CSV file
    converters = {}
    usecols = []
    for column in column_dtypes:
        converters[column] = str.strip
        usecols.append(column)

    # Read CSV file and strip whitespaces from all columns
    # os.path.join() doesn't work for S3 files when run from Windows
    file_path = f"{base_directory}/{file_relative_path}"
    df = pd.read_csv(file_path, sep=',', na_filter=False, usecols=usecols, converters=converters, encoding=encoding)

    # replace na_values with None
    to_replace = {na_value: None for na_value in na_values}
    df.replace(to_replace, inplace=True)

    # Convert columns to their specified types
    # for float, sets fractional or scientific notation values to NaN (?)
    for column, dtype in column_dtypes.items():
        if dtype == float:
            df.loc[:, column] = pd.to_numeric(df[column], errors='coerce')
        elif dtype == int:
            df.loc[:, column] = pd.to_numeric(df[column], errors='raise')
        elif dtype == 'category':
            df.loc[:, column] = df[column].astype('category')

    # Remove empty rows
    df.dropna(how="all", inplace=True)

    return df


def write_csv(df: pd.DataFrame,
              csv_name: str,
              compare: bool = False
              ) -> None:
    """Generic CSV Writer

    Args:
        df (pd.DatFrame): dataframe to write
        csv_name (str): prefix of output file name
        compare (bool): sort and compare output to pre-created sorted flow results (only works on Windows)
    """
    df_file_relative_path = f"{csv_name}.df.csv"
    df_file_path = f"{_DEBUG_BASE_DIRECTORY}/{df_file_relative_path}"

    # format float columns as string, ensuring NaNs are None and integers have no decimal point
    output_df = df.copy(deep=True)
    for column, dtype in output_df.dtypes.items():
        if dtype == float:
            condlist = [
                output_df[column].isna(),
                output_df[column].apply(float.is_integer),
                True
            ]
            choicelist = [
                None,
                output_df[column].fillna(0).astype(int).astype(str),
                output_df[column].astype(str)
            ]
            output_df.loc[:, column] = np.select(condlist, choicelist)

    output_df.to_csv(df_file_path, sep=',', index=False, encoding=_DEBUG_WRITE_ENCODING,
                     na_rep="", date_format=_CSV_DATE_FORMAT, chunksize=1_000_000)
    if compare:
        sorted_df_file_relative_path = f"{csv_name}.sorted.df.csv"
        sort_text_file(_DEBUG_BASE_DIRECTORY, df_file_relative_path, sorted_df_file_relative_path)
        sorted_flow_file_relative_path = f"{csv_name}.sorted.flow.csv"
        compare_text_files(_DEBUG_BASE_DIRECTORY, sorted_df_file_relative_path, sorted_flow_file_relative_path)


def compare_df_and_file(df: pd.DataFrame,
                        csv_name: str,
                        encoding: str = "utf-8-sig",
                        na_values: Optional[List[str]] = None):
    """Compare DataFrame with text file

    Args:
        df (pd.DataFrame): Dataframe to compare
        csv_name (str): Text file to compare
        encoding (str): Encoding for text file
        na_values (List[str]): Values in text file considered as NA
    """
    column_dtypes: ColumnDtypes = {}
    for column_name, column_dtype in df.dtypes.iteritems():
        dtype_name = column_dtype.name
        dtype = None
        if dtype_name == 'float64':
            dtype = float
        elif dtype_name == 'int64':
            dtype = int
        elif dtype_name == 'category':
            dtype = 'category'
        if dtype is not None:
            column_dtypes[str(column_name)] = dtype

    file_relative_path = f"{csv_name}.flow.csv"
    df2 = read_csv(base_directory=_DEBUG_BASE_DIRECTORY,
                   file_relative_path=file_relative_path,
                   encoding=encoding,
                   na_values=na_values if na_values is not None else [""],
                   column_dtypes=column_dtypes)

    if sorted(list(df.columns)) != sorted(list(df2.columns)):
        print('Dataframe and file do NOT have the same columns!')

    df2 = df2[df.columns]
    diff = df.compare(other=df2)
    if not diff.empty:
        print('Dataframe and file are NOT the same!')
        print(diff)
