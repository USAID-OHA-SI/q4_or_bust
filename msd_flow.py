from typing import List, Dict, Any

from datetime import datetime
import numpy as np
import pandas as pd

import utils

DATA_VERSION_KEY = 'data_version'
START_FISCAL_YEAR_KEY = 'start_fiscal_year'
CURRENT_FISCAL_YEAR_KEY = 'current_fiscal_year'
CURRENT_QUARTER_KEY = 'current_quarter'
DO_OUTPUT_COL_UPDATE_KEY = 'do_output_col_update'
FILE_LOCATORS_KEY = 'file_locators'
BASE_DIRECTORY_KEY = 'base_directory'
REF_TABLE_KEY = 'ref_table'
PARTNER_TYPE_TABLE_KEY = 'partner_type_table'
KNOWN_ISSUES_TABLE_KEY = 'known_issues_table'
PSNU_FILES_KEY = 'psnu_files'
NAT_SUBNAT_FILE_KEY = 'nat_subnat_file'
PATH_KEY = 'path'
ENCODING_KEY = 'encoding'
NA_VALUES_KEY = 'na_values'

_REF_TABLE_COLUMN_DTYPES: utils.ColumnDtypes = {
    'indicator (as seen in MSD)': 'category',
    'standardizeddisaggregate': 'category',
    'otherdisaggregate': 'category',
    'Fiscal Year (full year)': int,
    'Summed vs. Snapshot': 'category',
    'reporting_frequency': 'category',
}
_PARTNER_TYPE_TABLE_COLUMN_DTYPES: utils.ColumnDtypes = {
    'Mechanism ID': 'category',
    'Partner Type': 'category',
    'G2G': 'category',
}
_KNOWN_ISSUES_TABLE_COLUMN_DTYPES: utils.ColumnDtypes = {
    'period': 'category',
    'indicator': 'category',
    'operatingunit': 'category',
    'mech_code': 'category',
    'exclude due to known issue': 'category',
}
_PSNU_FILE_COLUMN_DTYPES: utils.ColumnDtypes = {
    'operatingunit': 'category',
    'operatingunituid': 'category',
    'countryname': 'category',
    'snu1': 'category',
    'snu1uid': 'category',
    'psnu': 'category',
    'psnuuid': 'category',
    'snuprioritization': 'category',
    'dreams': 'category',
    'primepartner': 'category',
    'fundingagency': 'category',
    'mech_code': 'category',
    'mech_name': 'category',
    'pre_rgnlztn_hq_mech_code': 'category',
    'prime_partner_duns': 'category',
    'award_number': 'category',
    'indicator': 'category',
    'numeratordenom': 'category',
    'indicatortype': 'category',
    'disaggregate': 'category',
    'standardizeddisaggregate': 'category',
    'categoryoptioncomboname': 'category',
    'ageasentered': 'category',
    'trendsfine': 'category',
    'trendssemifine': 'category',
    'trendscoarse': 'category',
    'sex': 'category',
    'statushiv': 'category',
    'statustb': 'category',
    'statuscx': 'category',
    'hiv_treatment_status': 'category',
    'otherdisaggregate': 'category',
    'otherdisaggregate_sub': 'category',
    'modality': 'category',
    'fiscal_year': int,
    'targets': float,
    'qtr1': float,
    'qtr2': float,
    'qtr3': float,
    'qtr4': float,
    'cumulative': float,
    'source_name': 'category',
}
_NAT_SUBNAT_FILE_COLUMN_DTYPES: utils.ColumnDtypes = {
    'operatingunit': 'category',
    'operatingunituid': 'category',
    'countryname': 'category',
    'snu1': 'category',
    'snu1uid': 'category',
    'psnu': 'category',
    'psnuuid': 'category',
    'snuprioritization': 'category',
    'indicator': 'category',
    'numeratordenom': 'category',
    'indicatortype': 'category',
    'disaggregate': 'category',
    'standardizeddisaggregate': 'category',
    'categoryoptioncomboname': 'category',
    'ageasentered': 'category',
    'trendscoarse': 'category',
    'sex': 'category',
    'statushiv': 'category',
    'otherdisaggregate': 'category',
    'fiscal_year': int,
    'targets': float,
    'qtr4': float,
    'source_name': 'category',
}

_OPERATINGUNIT_MAP = {
    "Democratic Republic of the Congo": "DRC",
    "Papua New Guinea": "PNG",
}
_COUNTRYNAME_MAP = _OPERATINGUNIT_MAP
_MODALITY_MAP = {
    "Inpat": "Inpatient",
    "HomeMod": "Community Home-Based",
    "Index": "Index (Facility)",
    "IndexMod": "Index (Community)",
    "MobileMod": "Community Mobile",
    "TBClinic": "TB Clinic",
    "OtherPITC": "Other PITC",
    "VCTMod": "Community VCT",
    "OtherMod": "Other Community",
    "Emergency Ward": "Emergency",
}

_SUM_VS_SNAP_FLOW_COL_ORDER = [
    'operatingunit',
    'operatingunituid',
    'countryname',
    'snu1',
    'snu1uid',
    'snuprioritization',
    'dreams',
    'psnu',
    'psnuuid',
    'fundingagency',
    'mech_name',
    'mech_code',
    'pre_rgnlztn_hq_mech_code',
    'award_number',
    'primepartner',
    'prime_partner_duns',
    'indicatortype',
    'indicator',
    'numeratordenom',
    'standardizeddisaggregate',
    'disaggregate',
    'otherdisaggregate',
    'otherdisaggregate_sub',
    'sex',
    'modality',
    'ageasentered',
    'trendsfine',
    'trendssemifine',
    'trendscoarse',
    'hiv_treatment_status',
    'statushiv',
    'statustb',
    'statuscx',
    'categoryoptioncomboname',
    'fiscal_year',
    'targets',
    'qtr1',
    'qtr2',
    'qtr3',
    'qtr4',
    'cumulative',
    'source_name',
    'Summed vs. Snapshot',
    'reporting_frequency',
]

_NET_NEW_TARGETS_FLOW_COL_ORDER = [
    '2022',
    '2021',
    '2020',
    '2019',
    '2018',
    'operatingunit',
    'operatingunituid',
    'countryname',
    'snu1',
    'snu1uid',
    'snuprioritization',
    'dreams',
    'psnu',
    'psnuuid',
    'fundingagency',
    'mech_name',
    'mech_code',
    'pre_rgnlztn_hq_mech_code',
    'award_number',
    'primepartner',
    'prime_partner_duns',
    'indicatortype',
    'indicator',
    'numeratordenom',
    'standardizeddisaggregate',
    'disaggregate',
    'otherdisaggregate',
    'otherdisaggregate_sub',
    'sex',
    'modality',
    'ageasentered',
    'trendsfine',
    'trendssemifine',
    'trendscoarse',
    'hiv_treatment_status',
    'statushiv',
    'statustb',
    'statuscx',
    'categoryoptioncomboname',
    'source_name',
    'Summed vs. Snapshot',
    'reporting_frequency',
]

_QTR_TARGETS_AND_CUMULATIVE_FLOW_COL_ORDER = [
    'qtr4|Running Cumulative',
    'qtr3|Running Cumulative',
    'qtr2|Running Cumulative',
    'qtr1|Running Cumulative',
    'qtr4|Targets (for Q. Ach)',
    'qtr3|Targets (for Q. Ach)',
    'qtr2|Targets (for Q. Ach)',
    'qtr1|Targets (for Q. Ach)',
    'operatingunit',
    'operatingunituid',
    'countryname',
    'snu1',
    'snu1uid',
    'snuprioritization',
    'dreams',
    'psnu',
    'psnuuid',
    'fundingagency',
    'mech_name',
    'mech_code',
    'pre_rgnlztn_hq_mech_code',
    'award_number',
    'primepartner',
    'prime_partner_duns',
    'indicatortype',
    'indicator',
    'numeratordenom',
    'standardizeddisaggregate',
    'disaggregate',
    'otherdisaggregate',
    'otherdisaggregate_sub',
    'sex',
    'modality',
    'ageasentered',
    'trendsfine',
    'trendssemifine',
    'trendscoarse',
    'hiv_treatment_status',
    'statushiv',
    'statustb',
    'statuscx',
    'categoryoptioncomboname',
    'fiscal_year',
    'targets',
    'qtr1',
    'qtr2',
    'qtr3',
    'qtr4',
    'cumulative',
    'source_name',
    'Summed vs. Snapshot',
    'reporting_frequency',
]

_REUNITE_FLOW_COL_ORDER = [
    'qtr1|Targets (for Q. Ach)',
    'qtr2|Targets (for Q. Ach)',
    'qtr3|Targets (for Q. Ach)',
    'qtr4|Targets (for Q. Ach)',
    'qtr4|Running Cumulative',
    'qtr3|Running Cumulative',
    'qtr2|Running Cumulative',
    'qtr1|Running Cumulative',
    'fiscal_year',
    'targets',
    'qtr1',
    'qtr2',
    'qtr3',
    'cumulative',
    'operatingunit',
    'operatingunituid',
    'countryname',
    'snu1',
    'snu1uid',
    'snuprioritization',
    'dreams',
    'psnu',
    'psnuuid',
    'fundingagency',
    'mech_name',
    'mech_code',
    'pre_rgnlztn_hq_mech_code',
    'award_number',
    'primepartner',
    'prime_partner_duns',
    'indicatortype',
    'indicator',
    'numeratordenom',
    'standardizeddisaggregate',
    'disaggregate',
    'otherdisaggregate',
    'otherdisaggregate_sub',
    'sex',
    'modality',
    'ageasentered',
    'trendsfine',
    'trendssemifine',
    'trendscoarse',
    'hiv_treatment_status',
    'statushiv',
    'statustb',
    'statuscx',
    'categoryoptioncomboname',
    'source_name',
    'Summed vs. Snapshot',
    'reporting_frequency',
    'qtr4',
]

_CLEAN_FIELDS_COL_UPDATE_MAP = {
    'Results': 'results',
    'Targets': 'targets',
    'Cumulative': 'cumulative',
}

_CLEAN_FIELDS_FLOW_COL_ORDER = [
    'quarter',
    'key_pops',
    'community_facility',
    'index',
    'Results or Targets',
    'FY',
    'values',
    'cumulative',
    'targets',
    'results',
    'Running Cumulative',
    'Targets (for Q. Ach)',
    'operatingunit',
    'operatingunituid',
    'countryname',
    'snu1',
    'snu1uid',
    'snuprioritization',
    'dreams',
    'psnu',
    'psnuuid',
    'fundingagency',
    'mech_name',
    'mech_code',
    'pre_rgnlztn_hq_mech_code',
    'award_number',
    'primepartner',
    'prime_partner_duns',
    'indicatortype',
    'indicator',
    'numeratordenom',
    'standardizeddisaggregate',
    'disaggregate',
    'otherdisaggregate',
    'otherdisaggregate_sub',
    'sex',
    'modality',
    'ageasentered',
    'trendsfine',
    'trendssemifine',
    'trendscoarse',
    'hiv_treatment_status',
    'statushiv',
    'statustb',
    'statuscx',
    'categoryoptioncomboname',
    'source_name',
    'Summed vs. Snapshot',
    'reporting_frequency',
]

_UPDATE_FIELD_NAMES_COL_UPDATE_MAP = {
    'statushiv': 'Status HIV',
    'statustb': 'Status TB',
    'statuscx': 'Status CX',
    'categoryoptioncomboname': 'category_option_combo_name',
    'hiv_treatment_status': 'HIV_treatment_status',
    'ageasentered': 'age_as_entered',
    'trendscoarse': 'age_coarse',
    'trendssemifine': 'age_semifine',
    'trendsfine': 'age_fine',
    'otherdisaggregate': 'other_disaggregate',
    'numeratordenom': 'numerator_denom',
    'indicatortype': 'indicator_type',
    'primepartner': 'prime_partner',
    'fundingagency': 'funding_agency',
    'countryname': 'country_name',
    'snuprioritization': 'SNU_prioritization',
    'snu1uid': 'SNU UID',
    'snu1': 'SNU',
    'psnu': 'PSNU',
    'psnuuid': 'PSNU UID',
    'operatingunituid': 'operating_unit_uid',
    'operatingunit': 'operating_unit',
    'dreams': 'DREAMS',
    'standardizeddisaggregate': 'standardized_disaggregate',
}

_UPDATE_FIELD_NAMES_FLOW_COL_ORDER = [
    'operating_unit',
    'country_name',
    'modality',
    'funding_agency',
    'quarter',
    'key_pops',
    'community_facility',
    'index',
    'Current Quarter',
    'Results or Targets',
    'FY',
    'values',
    'cumulative',
    'targets',
    'results',
    'Running Cumulative',
    'Targets (for Q. Ach)',
    'operating_unit_uid',
    'SNU',
    'SNU UID',
    'PSNU',
    'PSNU UID',
    'SNU_prioritization',
    'indicator',
    'numerator_denom',
    'indicator_type',
    'disaggregate',
    'standardized_disaggregate',
    'category_option_combo_name',
    'age_as_entered',
    'age_coarse',
    'sex',
    'Status HIV',
    'other_disaggregate',
    'source_name',
    'DREAMS',
    'mech_name',
    'mech_code',
    'pre_rgnlztn_hq_mech_code',
    'award_number',
    'prime_partner',
    'prime_partner_duns',
    'otherdisaggregate_sub',
    'age_fine',
    'age_semifine',
    'HIV_treatment_status',
    'Status TB',
    'Status CX',
]

_PARTNER_TYPE_FLOW_COL_ORDER = [
    'Partner Type',
    'G2G',
    'indicator',
    'community_facility',
    'modality',
    'otherdisaggregate_sub',
    'mech_code',
    'pre_rgnlztn_hq_mech_code',
    'SNU UID',
    'standardized_disaggregate',
    'prime_partner_duns',
    'results',
    'SNU_prioritization',
    'Running Cumulative',
    'index',
    'PSNU',
    'Status TB',
    'operating_unit_uid',
    'age_coarse',
    'HIV_treatment_status',
    'Targets (for Q. Ach)',
    'PSNU UID',
    'award_number',
    'prime_partner',
    'values',
    'indicator_type',
    'targets',
    'age_fine',
    'age_semifine',
    'Results or Targets',
    'Status CX',
    'other_disaggregate',
    'disaggregate',
    'FY',
    'country_name',
    'age_as_entered',
    'funding_agency',
    'SNU',
    'numerator_denom',
    'source_name',
    'mech_name',
    'Status HIV',
    'sex',
    'cumulative',
    'category_option_combo_name',
    'operating_unit',
    'DREAMS',
    'key_pops',
    'Current Quarter',
    'quarter',
]

_KNOWN_ISSUES_FLOW_COL_ORDER = [
    'Partner Type',
    'G2G',
    'exclude due to known issue',
    'indicator',
    'community_facility',
    'modality',
    'otherdisaggregate_sub',
    'mech_code',
    'pre_rgnlztn_hq_mech_code',
    'SNU UID',
    'standardized_disaggregate',
    'prime_partner_duns',
    'results',
    'SNU_prioritization',
    'Running Cumulative',
    'index',
    'PSNU',
    'Status TB',
    'operating_unit_uid',
    'age_coarse',
    'HIV_treatment_status',
    'Targets (for Q. Ach)',
    'PSNU UID',
    'award_number',
    'prime_partner',
    'values',
    'indicator_type',
    'targets',
    'age_fine',
    'age_semifine',
    'Results or Targets',
    'Status CX',
    'other_disaggregate',
    'disaggregate',
    'FY',
    'country_name',
    'age_as_entered',
    'funding_agency',
    'SNU',
    'numerator_denom',
    'source_name',
    'mech_name',
    'Status HIV',
    'sex',
    'cumulative',
    'category_option_combo_name',
    'operating_unit',
    'DREAMS',
    'key_pops',
    'Current Quarter',
    'quarter',
]

_REMOVE_FUTURE_YEARS_FLOW_COL_ORDER = [
    'FY',
    'quarter',
    'exclude due to known issue',
    'Partner Type',
    'G2G',
    'indicator',
    'community_facility',
    'modality',
    'otherdisaggregate_sub',
    'mech_code',
    'pre_rgnlztn_hq_mech_code',
    'SNU UID',
    'standardized_disaggregate',
    'prime_partner_duns',
    'results',
    'SNU_prioritization',
    'Running Cumulative',
    'index',
    'PSNU',
    'Status TB',
    'operating_unit_uid',
    'age_coarse',
    'HIV_treatment_status',
    'Targets (for Q. Ach)',
    'PSNU UID',
    'award_number',
    'prime_partner',
    'values',
    'indicator_type',
    'targets',
    'age_fine',
    'age_semifine',
    'Results or Targets',
    'Status CX',
    'other_disaggregate',
    'disaggregate',
    'country_name',
    'age_as_entered',
    'funding_agency',
    'SNU',
    'numerator_denom',
    'source_name',
    'mech_name',
    'Status HIV',
    'sex',
    'cumulative',
    'category_option_combo_name',
    'operating_unit',
    'DREAMS',
    'key_pops',
    'Current Quarter',
]

_HYPER_ORDER_AND_POSTGRES_COL_UPDATE_MAP = {
    'FY': 'fy',
    'quarter': 'quarter',
    'values': 'values',
    'Targets (for Q. Ach)': 'targets_for_q_ach',
    'Running Cumulative': 'running_cumulative',
    'targets': 'targets',
    'operating_unit': 'operating_unit',
    'country_name': 'country_name',
    'modality': 'modality',
    'index': 'index',
    'community_facility': 'community_facility',
    'key_pops': 'key_pops',
    'funding_agency': 'funding_agency',
    'Current Quarter': 'current_quarter',
    'cumulative': 'cumulative',
    'results': 'results',
    'Results or Targets': 'results_or_targets',
    'operating_unit_uid': 'operating_unit_uid',
    'SNU': 'snu',
    'SNU UID': 'snu_uid',
    'SNU_prioritization': 'snu_prioritization',
    'DREAMS': 'dreams',
    'PSNU': 'psnu',
    'PSNU UID': 'psnu_uid',
    'mech_name': 'mech_name',
    'mech_code': 'mech_code',
    'pre_rgnlztn_hq_mech_code': 'pre_rgnlztn_hq_mech_code',
    'award_number': 'award_number',
    'prime_partner': 'prime_partner',
    'prime_partner_duns': 'prime_partner_duns',
    'indicator_type': 'indicator_type',
    'indicator': 'indicator',
    'numerator_denom': 'numerator_denom',
    'standardized_disaggregate': 'standardized_disaggregate',
    'disaggregate': 'disaggregate',
    'other_disaggregate': 'other_disaggregate',
    'otherdisaggregate_sub': 'otherdisaggregate_sub',
    'sex': 'sex',
    'age_as_entered': 'age_as_entered',
    'age_fine': 'age_fine',
    'age_semifine': 'age_semifine',
    'age_coarse': 'age_coarse',
    'HIV_treatment_status': 'hiv_treatment_status',
    'Status HIV': 'status_hiv',
    'Status TB': 'status_tb',
    'Status CX': 'status_cx',
    'category_option_combo_name': 'category_option_combo_name',
    'source_name': 'source_name',
    'Partner Type': 'partner_type',
    'G2G': 'g2g',
    'exclude due to known issue': 'exclude_due_to_known_issue',
}


def _import_reference_table(base_directory: str, ref_table: Dict[str, Any]) -> pd.DataFrame:
    """Import Reference Table."""
    # Read CSV file
    df = utils.read_csv(base_directory=base_directory,
                        file_relative_path=ref_table[PATH_KEY],
                        encoding=ref_table[ENCODING_KEY],
                        na_values=ref_table[NA_VALUES_KEY],
                        column_dtypes=_REF_TABLE_COLUMN_DTYPES)
    return df


def _import_partner_type_table(base_directory: str, partner_type_table: Dict[str, Any]) -> pd.DataFrame:
    """Import Reference Table."""
    # Read CSV file
    df = utils.read_csv(base_directory=base_directory,
                        file_relative_path=partner_type_table[PATH_KEY],
                        encoding=partner_type_table[ENCODING_KEY],
                        na_values=partner_type_table[NA_VALUES_KEY],
                        column_dtypes=_PARTNER_TYPE_TABLE_COLUMN_DTYPES)
    return df


def _import_known_issues_table(base_directory: str, known_issues_table: Dict[str, Any]) -> pd.DataFrame:
    """Import Reference Table."""
    # Read CSV file
    df = utils.read_csv(base_directory=base_directory,
                        file_relative_path=known_issues_table[PATH_KEY],
                        encoding=known_issues_table[ENCODING_KEY],
                        na_values=known_issues_table[NA_VALUES_KEY],
                        column_dtypes=_KNOWN_ISSUES_TABLE_COLUMN_DTYPES)
    return df


def _import_psnu_files(base_directory: str, psnu_files: List[Dict[str, Any]]) -> pd.DataFrame:
    """Read and merge all PSNU files."""
    # Read all CSV files
    dfs = []
    for psnu_file in psnu_files:
        df = utils.read_csv(base_directory=base_directory,
                            file_relative_path=psnu_file[PATH_KEY],
                            encoding=psnu_file[ENCODING_KEY],
                            na_values=psnu_file[NA_VALUES_KEY],
                            column_dtypes=_PSNU_FILE_COLUMN_DTYPES)

        # convert [mech_code] to match flow, removing leading zeroes and changing non-numeric to None
        utils.set_category_column(df=df, column='mech_code',
                                  values=df['mech_code'].apply(lambda x: str(int(x)) if x.isnumeric() else None),
                                  recalc_categories=True)

        dfs.append(df)

    # Merge all dataframes into single dataframe
    df = utils.concatenate_dfs(dfs)
    del dfs

    return df


def _import_nat_subnat_file(base_directory: str, nat_subnat_file: Dict[str, Any]) -> pd.DataFrame:
    """Read Nat/Subnat file."""
    df = utils.read_csv(base_directory=base_directory,
                        file_relative_path=nat_subnat_file[PATH_KEY],
                        encoding=nat_subnat_file[ENCODING_KEY],
                        na_values=nat_subnat_file[NA_VALUES_KEY],
                        column_dtypes=_NAT_SUBNAT_FILE_COLUMN_DTYPES)
    return df


def _sum_vs_snap(input_df: pd.DataFrame, lookup_df: pd.DataFrame) -> pd.DataFrame:
    """Join PSNU data with Reference Table, adding [sum vs. snapshot] and [reporting_frequency] columns."""
    # Join (Left)
    dfs = [input_df, lookup_df]
    utils.unify_categories(dfs, columns=['indicator', 'indicator (as seen in MSD)'])
    utils.unify_categories(dfs, column='standardizeddisaggregate')
    utils.unify_categories(dfs, column='otherdisaggregate')
    df = input_df.merge(lookup_df,
                        left_on=['indicator', 'fiscal_year',
                                 'standardizeddisaggregate', 'otherdisaggregate'],
                        right_on=['indicator (as seen in MSD)', 'Fiscal Year (full year)',
                                  'standardizeddisaggregate', 'otherdisaggregate'],
                        how='left'
                        )

    # Remove Field: [indicator (as seen in MSD)], [Fiscal Year (full year)]
    df.drop(columns=['indicator (as seen in MSD)', 'Fiscal Year (full year)'], inplace=True)

    # Reorder columns to match Tableau flow output
    df = df.reindex(columns=_SUM_VS_SNAP_FLOW_COL_ORDER)

    return df


def _net_new_targets(input_df: pd.DataFrame, start_fiscal_year: int, target_fiscal_year: int) -> pd.DataFrame:
    """Calculate Net New Targets."""
    df = input_df.copy(deep=False)

    df = df[df['fiscal_year'] >= start_fiscal_year]
    df = df[df['indicator'] == "TX_CURR"]
    utils.set_category_column(df=df, column='indicator', value="TX_NET_NEW")
    df = df[df['standardizeddisaggregate'] != "Age/Sex/ARVDispense/HIVStatus"]
    df = df[~(df['targets'].isna() & df['cumulative'].isna())]

    loc = 0
    new_cols = []
    for year in range(start_fiscal_year, target_fiscal_year):
        df[f'{year} Cumulative'] = df['cumulative'].where(cond=df['fiscal_year'] == year, other=np.NaN)
        df[f'{year + 1} Targets'] = df['targets'].where(cond=df['fiscal_year'] == year + 1, other=np.NaN)
        new_col = f'{year + 1}'
        df.insert(loc, new_col, df[f'{year + 1} Targets'].fillna(0) - df[f'{year} Cumulative'].fillna(0))
        new_cols.append(new_col)
        df.drop(columns=[f'{year} Cumulative', f'{year + 1} Targets'], inplace=True)
        loc += 1

    df.drop(columns=['fiscal_year', 'targets', 'cumulative', 'qtr1', 'qtr2', 'qtr3', 'qtr4'], inplace=True)

    # the following returns an empty df, perhaps too many columns
    # df = df.groupby(list(df.columns.difference(new_cols)), observed=True, as_index=False).sum()
    unique_df = df.drop(columns=new_cols).drop_duplicates().reset_index()
    indexed_df = unique_df.merge(df, how="left")[['index'] + new_cols]
    agg_df = indexed_df.groupby('index', as_index=False).sum()
    df = unique_df.merge(agg_df, how="left").drop(columns=['index'])

    return df


def _make_fy_rows(input_df: pd.DataFrame, start_fiscal_year: int, target_fiscal_year: int) -> pd.DataFrame:
    """Create separate row per year."""
    value_vars = [f"{year + 1}" for year in range(start_fiscal_year, target_fiscal_year)]
    id_vars = input_df.columns.difference(value_vars, sort=False)
    df = input_df.melt(id_vars=id_vars, value_vars=value_vars, var_name='fiscal_year', value_name='targets')
    df.loc[:, 'fiscal_year'] = pd.to_numeric(df['fiscal_year'], errors='raise')

    # Match Tableau flow output
    utils.move_column(df, "fiscal_year", 0)
    utils.move_column(df, "targets", 0)

    # Filter (exclude)
    df = df[~(df['targets'].isna() | (df['targets'] == 0))]

    # create qtr columns
    for qtr in range(1, 5):
        df.insert(0, f'qtr{qtr}|Targets (for Q. Ach)', df['targets'])

    return df


def _qtr_targets_and_cumulative(input_df: pd.DataFrame) -> pd.DataFrame:
    """Create quarterly targets & running cumulative."""
    df = input_df.copy(deep=False)

    # Calculated Field: [indicator] (update)
    condlist = [
        (df['indicator'] == "TX_CURR") & (df['disaggregate'] == "Age/Sex/ARVDispense/HIVStatus"),
        (df['indicator'] == "PMTCT_HEI_POS") & (df['disaggregate'] == "Age/HIVStatus/ARTStatus"),
        True
    ]
    choicelist = [
        "TX_MMD",
        "PMTCT_HEI_POS_ART",
        df['indicator']
    ]
    utils.add_categories(df=df, column='indicator', categories=["TX_MMD", "PMTCT_HEI_POS_ART"])
    utils.set_category_column(df=df, column='indicator', values=np.select(condlist, choicelist))
    del condlist, choicelist

    # create qtr columns
    for qtr in range(1, 5):
        df.insert(0, f'qtr{qtr}|Targets (for Q. Ach)', df['targets'])

    # create qtr|Running Cumulative columns
    df.insert(0, 'qtr1|Running Cumulative', df['qtr1'])

    cond = df['Summed vs. Snapshot'] == "Snapshot"
    other = df['qtr1'].fillna(0) + df['qtr2'].fillna(0)
    df.insert(0, 'qtr2|Running Cumulative', df['qtr2'].where(cond=cond, other=other))
    del cond, other

    cond = (df['Summed vs. Snapshot'] == "Snapshot") | \
           ((df['Summed vs. Snapshot'] == "Summed") & (df['reporting_frequency'] == "Semi-Annual"))
    other = df['qtr1'].fillna(0) + df['qtr2'].fillna(0) + df['qtr3'].fillna(0)
    df.insert(0, 'qtr3|Running Cumulative', df['qtr3'].where(cond=cond, other=other))
    del cond, other

    cond = df['Summed vs. Snapshot'] == "Snapshot"
    other = df['qtr1'].fillna(0) + df['qtr2'].fillna(0) + df['qtr3'].fillna(0) + df['qtr4'].fillna(0)
    df.insert(0, 'qtr4|Running Cumulative', df['qtr4'].where(cond=cond, other=other))
    del cond, other

    # Reorder columns to match Tableau flow output
    df = df.reindex(columns=_QTR_TARGETS_AND_CUMULATIVE_FLOW_COL_ORDER)

    return df


def _reunite(main_df: pd.DataFrame, net_new_df: pd.DataFrame, nat_subnat_df: pd.DataFrame) -> pd.DataFrame:
    """Create merged dataframe."""
    # Merge dataframes
    df = utils.concatenate_dfs([main_df, net_new_df, nat_subnat_df])

    # Reorder columns to match Tableau flow output
    df = df.reindex(columns=_REUNITE_FLOW_COL_ORDER)
    return df


def _make_long(input_df: pd.DataFrame) -> pd.DataFrame:
    """Pivot all numeric values."""
    value_vars = input_df.columns[input_df.dtypes.values == np.dtype('float64')]
    id_vars = input_df.columns.difference(value_vars, sort=False)
    df = input_df.melt(id_vars=id_vars, value_vars=value_vars, var_name='quarter', value_name='values')
    df.loc[:, 'quarter'] = df['quarter'].astype('category')

    # Match Tableau flow output
    utils.move_column(df, "values", 0)
    utils.move_column(df, "quarter", 0)

    return df


def _clean_fields(input_df: pd.DataFrame) -> pd.DataFrame:
    """Cleaning data."""
    df = input_df.copy(deep=False)

    # Filter (exclude)
    df = df[~df['values'].isna()]

    # Filter (exclude)
    quarter_mask = df['quarter'].isin(
        [
            "targets",
            "qtr1|Targets (for Q. Ach)", "qtr2|Targets (for Q. Ach)",
            "qtr3|Targets (for Q. Ach)", "qtr4|Targets (for Q. Ach)",
            "cumulative",
            "qtr1|Running Cumulative", "qtr2|Running Cumulative",
            "qtr3|Running Cumulative", "qtr4|Running Cumulative",
        ]
    )
    values_mask = df['values'] == 0
    df = df[~(quarter_mask & values_mask)]
    del quarter_mask, values_mask

    # Calculated Field: [Results or Targets] (insert)
    condlist = [
        df['quarter'].str.contains('|', regex=False, na=False),
        df['quarter'] == "cumulative",
        df['quarter'] == "targets",
        True
    ]
    choicelist = [
        df['quarter'].str.split("|", 1).str[1],
        "Cumulative",
        "Targets",
        "Results"
    ]
    utils.set_category_column(df=df, column='Results or Targets', values=np.select(condlist, choicelist))
    del condlist, choicelist

    # Calculated Field: [FY] (insert)
    utils.set_category_column(df=df, column='FY', values=df['fiscal_year'].apply(utils.fy_from_fiscal_year))

    # Calculated Field: [quarter] (update)
    utils.set_category_column(df=df, column='quarter', values=df['quarter'].str.split('|', 1).str[0],
                              recalc_categories=True)

    # Calculated Field: [quarter] (update)
    condlist = [
        df['quarter'].str[:3] == "qtr",
        df['quarter'] == "targets",
        df['quarter'] == "cumulative",
        True
    ]
    choicelist = [
        df['FY'].astype(str) + " Q" + df['quarter'].str[3],
        df['FY'].astype(str) + " Targets",
        df['FY'].astype(str) + " Cumulative",
        "ERROR"
    ]
    utils.set_category_column(df=df, column='quarter', values=np.select(condlist, choicelist),
                              recalc_categories=True)
    del condlist, choicelist

    # Create columns for [Results or Targets] values
    df_pivot = df.pivot(columns='Results or Targets', values='values')
    df_pivot.rename(columns=_CLEAN_FIELDS_COL_UPDATE_MAP, inplace=True)
    df = df.join(df_pivot, how='outer')

    # Filter (exclude)
    df = df[~((df['Results or Targets'] == "Running Cumulative") &
              (df['values'] == 0) &
              df['cumulative'].isna() &
              df['results'].isna()
              )]

    # Remove Field: [fiscal_year]
    df.drop(columns=['fiscal_year'], inplace=True)

    # Calculated Field: [operatingunit] (update)
    utils.set_category_column(df=df, column='operatingunit', value_map=_OPERATINGUNIT_MAP)

    # Calculated Field: [countryname] (update)
    utils.set_category_column(df=df, column='countryname', value_map=_COUNTRYNAME_MAP)

    # Calculated Field: [modality] (update)
    utils.set_category_column(df=df, column='modality', value_map=_MODALITY_MAP)

    # Calculated Field: [index] (insert)
    condlist = [
        df['standardizeddisaggregate'] == "3:Age Aggregated/Sex/Contacts",
        df['standardizeddisaggregate'] == "2:Age/Sex/IndexCasesAccepted",
        df['standardizeddisaggregate'] == "1:Age/Sex/IndexCasesOffered",
        ((df['standardizeddisaggregate'] == "4:Age/Sex/Result") &
         (df['otherdisaggregate'] == "Known at Entry")),
        ((df['standardizeddisaggregate'] == "4:Age/Sex/Result") &
         (df['otherdisaggregate'] == "Newly Identified") &
         (df['statushiv'] == "Positive")),
        ((df['standardizeddisaggregate'] == "4:Age/Sex/Result") &
         (df['otherdisaggregate'] == "Newly Identified") &
         (df['statushiv'] == "Negative")),
        True
    ]
    choicelist = [
        "Contacts",
        "Accepted",
        "Offered",
        "Not Tested - positive at entry",
        "Tested - positive",
        "Tested - negative",
        None
    ]
    utils.set_category_column(df=df, column='index', values=np.select(condlist, choicelist))
    del condlist, choicelist

    # Calculated Field: [community_facility] (insert)
    condlist = [
        df['disaggregate'].str.contains("IndexMod/", regex=False, na=False),
        df['disaggregate'].str.contains("Index/", regex=False, na=False),
        True
    ]
    choicelist = [
        "Community",
        "Facility",
        None
    ]
    utils.set_category_column(df=df, column='community_facility', values=np.select(condlist, choicelist))
    del condlist, choicelist

    # Calculated Field: [key_pops] (insert)
    condlist = [
        df['otherdisaggregate'].str.contains("MSM", regex=False, na=False),
        df['otherdisaggregate'].str.contains("FSW", regex=False, na=False),
        df['otherdisaggregate'].str.contains("PWID", regex=False, na=False),
        df['otherdisaggregate'].str.contains("People in prisons", regex=False, na=False),
        df['otherdisaggregate'].str.contains("TG", regex=False, na=False),
        df['otherdisaggregate'] == "Other Key Populations",
        True
    ]
    choicelist = [
        "MSM",
        "FSW",
        "PWID",
        "People in prisons",
        "TG",
        "Other key populations",
        None
    ]
    utils.set_category_column(df=df, column='key_pops', values=np.select(condlist, choicelist))
    del condlist, choicelist

    # Calculated Field: [fundingagency] (update)
    condlist = [
        df['fundingagency'] == "PC",
        df['fundingagency'].str.contains("CDC", regex=False, na=False),
        df['fundingagency'].str.contains("HRSA", regex=False, na=False),
        df['fundingagency'].str.contains("State", regex=False, na=False),
        True
    ]
    choicelist = [
        "Peace Corps",
        "CDC",
        "HRSA",
        "State Dept.",
        df['fundingagency']
    ]
    utils.add_categories(df=df, column='fundingagency', categories=["Peace Corps", "CDC", "HRSA", "State Dept."])
    utils.set_category_column(df=df, column='fundingagency', values=np.select(condlist, choicelist))
    del condlist, choicelist

    # Reorder columns to match flow output
    df = df.reindex(columns=_CLEAN_FIELDS_FLOW_COL_ORDER)
    return df


def _update_field_names(input_df: pd.DataFrame, current_fyq: str) -> pd.DataFrame:
    df = input_df.copy(deep=False)

    # Remove Field: [Summed vs. Snapshot], [reporting_frequency]
    df.drop(columns=['Summed vs. Snapshot', 'reporting_frequency'], inplace=True)

    # Calculated Field: [Current Quarter] (insert)
    utils.set_category_column(df=df, column='Current Quarter', value=current_fyq)

    # Rename Fields
    df.rename(columns=_UPDATE_FIELD_NAMES_COL_UPDATE_MAP,
              inplace=True)

    # Reorder columns to match Tableau flow output
    df = df.reindex(columns=_UPDATE_FIELD_NAMES_FLOW_COL_ORDER)

    return df


def _partner_type(input_df: pd.DataFrame, lookup_df: pd.DataFrame) -> pd.DataFrame:
    """Join Main and PartnerType Table, adding [Partner Type] and [G2G] columns."""
    # Join (Left)
    dfs = [input_df, lookup_df]
    utils.unify_categories(dfs, columns=['mech_code', 'Mechanism ID'])
    df = input_df.merge(lookup_df,
                        left_on=['mech_code'],
                        right_on=['Mechanism ID'],
                        how='left'
                        )

    # Remove Field: [Mechanism ID]
    df.drop(columns=['Mechanism ID'], inplace=True)

    # Reorder columns to match Tableau flow output
    df = df.reindex(columns=_PARTNER_TYPE_FLOW_COL_ORDER)

    return df


def _known_issues(input_df: pd.DataFrame, lookup_df: pd.DataFrame) -> pd.DataFrame:
    """Join Main and Known Issues Table, adding [exclude due to known issue]."""
    # Join (Left)
    dfs = [input_df, lookup_df]
    utils.unify_categories(dfs, column='indicator')
    utils.unify_categories(dfs, column='mech_code')
    utils.unify_categories(dfs, columns=['operating_unit', 'operatingunit'])
    utils.unify_categories(dfs, columns=['quarter', 'period'])
    df = input_df.merge(lookup_df,
                        left_on=['indicator', 'mech_code', 'operating_unit', 'quarter'],
                        right_on=['indicator', 'mech_code', 'operatingunit', 'period'],
                        how='left'
                        )

    # Remove Fields: ['period', 'operatingunit]
    df.drop(columns=['period', 'operatingunit'], inplace=True)

    # Reorder columns to match Tableau flow output
    df = df.reindex(columns=_KNOWN_ISSUES_FLOW_COL_ORDER)

    return df


def _create_calendar_date(input_df: pd.DataFrame) -> pd.DataFrame:
    df = input_df.copy(deep=False)

    # Calculated Field: [quarter calendar date] (insert)
    df.loc[:, 'quarter calendar date'] = df['quarter'].apply(utils.date_from_fyq)

    # Match Tableau flow output
    utils.move_column(df, "quarter calendar date", 1)

    return df


def _vlc(input_df: pd.DataFrame) -> pd.DataFrame:
    df = input_df.copy(deep=False)

    # Filter (include)
    df = df[df['indicator'].isin(["PMTCT_ART", "TX_CURR", "TX_PVLS"])]

    # Filter (include)
    df = df[df['Results or Targets'] == "Results"]

    # Filter (exclude)
    df = df[~((df['indicator'] == "TX_PVLS") & (df['numerator_denom'] == "N"))]

    # Filter (exclude)
    df = df[~((df['indicator'] == "PMTCT_ART") & (df['numerator_denom'] == "D"))]

    # Filter (exclude)
    other_disaggregate_mask = df['other_disaggregate'].isin(
        [
            "AZT",
            "Breastfeeding, Routine",
            "Breastfeeding, Targeted",
            "Breastfeeding, Undocumented Test Indication",
            "Life-long ART, New",
            "Single-dose NVP",
            "Triple-drug ARV",
        ]
    )
    df = df[~other_disaggregate_mask]
    del other_disaggregate_mask

    # Filter (exclude)
    df = df[~((df['indicator'] == "PMTCT_ART") & (df['standardized_disaggregate'] == "Total Numerator"))]

    # Calculated Field: [VLC_GP Denominator 0], [...-1], [...-2], [...-3] (insert)
    cond = (df['indicator'] == "PMTCT_ART") & (df['numerator_denom'] == "N")
    values = df['quarter calendar date']
    df.loc[:, 'VLC_GP Denominator 0'] = values.where(cond=cond, other=pd.NaT)

    values = df['quarter calendar date'] + pd.DateOffset(months=3)
    df.loc[:, 'VLC_GP Denominator -1'] = values.where(cond=cond, other=pd.NaT)

    values = df['quarter calendar date'] + pd.DateOffset(months=6)
    df.loc[:, 'VLC_GP Denominator -2'] = values.where(cond=cond, other=pd.NaT)

    values = df['quarter calendar date'] + pd.DateOffset(months=9)
    df.loc[:, 'VLC_GP Denominator -3'] = values.where(cond=cond, other=pd.NaT)
    del cond, values

    # Calculated Field: [VLC_GP Denominator -2_] (insert)
    cond = (df['indicator'] == "TX_CURR") & (df['numerator_denom'] == "N")
    values = df['quarter calendar date'] + pd.DateOffset(months=6)
    df.loc[:, 'VLC_GP Denominator -2_'] = values.where(cond=cond, other=pd.NaT)
    del cond, values

    # Calculated Field: [VLC_GP Numerator 0] (insert)
    cond = (df['indicator'] == "TX_PVLS") & (df['numerator_denom'] == "D")
    values = df['quarter calendar date']
    df.loc[:, 'VLC_GP Numerator 0'] = values.where(cond=cond, other=pd.NaT)
    del cond, values

    # Match Tableau flow output
    utils.move_column(df, "VLC_GP Numerator 0", 0)
    utils.move_column(df, "VLC_GP Denominator -3", 0)
    utils.move_column(df, "VLC_GP Denominator -2", 0)
    utils.move_column(df, "VLC_GP Denominator -1", 0)
    utils.move_column(df, "VLC_GP Denominator 0", 0)
    utils.move_column(df, "VLC_GP Denominator -2_", 0)

    # Group Values: [standardized_disaggregate]
    condlist = [
        (df['standardized_disaggregate'] == "Total Denominator"),
        (df['standardized_disaggregate'] == "Total Numerator"),
        True
    ]
    choicelist = [
        "Total Numerator",
        "Total Denominator",
        df['standardized_disaggregate']
    ]
    utils.set_category_column(df=df, column='standardized_disaggregate', values=np.select(condlist, choicelist))
    del condlist, choicelist

    # Group Values: [disaggregate]
    condlist = [
        (df['disaggregate'] == "Total Denominator"),
        (df['disaggregate'] == "Total Numerator"),
        True
    ]
    choicelist = [
        "Total Numerator",
        "Total Denominator",
        df['disaggregate']
    ]
    utils.set_category_column(df=df, column='disaggregate', values=np.select(condlist, choicelist))
    del condlist, choicelist

    # Group Values: [numerator_denom]
    condlist = [
        (df['numerator_denom'] == "D"),
        (df['numerator_denom'] == "N"),
        True
    ]
    choicelist = [
        "N",
        "D",
        df['numerator_denom']
    ]
    utils.set_category_column(df=df, column='numerator_denom', values=np.select(condlist, choicelist))
    del condlist, choicelist

    # Calculated Field: [indicator] (update)
    utils.set_category_column(df=df, column='indicator', value="TX_VL_COVERAGE")

    # Calculated Field: [source_name] (update)
    utils.set_category_column(df=df, column='source_name', value="Derived")

    # Remove Field: [quarter calendar date], [quarter], [FY]
    df.drop(columns=['quarter calendar date', 'quarter', 'FY'], inplace=True)

    return df


def _dates_to_rows(input_df: pd.DataFrame) -> pd.DataFrame:
    """Pivot columns to rows, naming them [quarter calendar date]."""
    value_vars = ['VLC_GP Denominator -1', 'VLC_GP Denominator -2', 'VLC_GP Denominator -3',
                  'VLC_GP Denominator 0', 'VLC_GP Numerator 0', 'VLC_GP Denominator -2_']
    id_vars = input_df.columns.difference(value_vars, sort=False)
    df = input_df.melt(id_vars=id_vars, value_vars=value_vars, var_name='pivot', value_name='quarter calendar date')

    # Match Tableau flow output
    utils.move_column(df, "quarter calendar date", 0)

    # Drop Field: [pivot_vars]
    df.drop(columns=['pivot'], inplace=True)

    # Filter (Exlude)
    df = df[~df['quarter calendar date'].isna()]

    return df


def _correct_fyq(input_df: pd.DataFrame, current_fyq_start_date: datetime) -> pd.DataFrame:
    """Calculate in "FYxx Qx" (fyq) format."""
    df = input_df.copy(deep=False)

    # Filter (Exlude)
    df = df[~(df['quarter calendar date'] > current_fyq_start_date)]

    # Calculated Field: [quarter] (insert)
    utils.set_category_column(df=df, column='quarter',
                              values=df['quarter calendar date'].apply(utils.fyq_from_date))

    # Calculated Field: [FY] (insert)
    utils.set_category_column(df=df, column='FY',
                              values=df['quarter'].apply(lambda x: utils.fy_and_quarter_from_fyq(x)[0]))

    # Match Tableau flow output
    utils.move_column(df, "quarter", 0)
    utils.move_column(df, "FY", 0)

    return df


def _prev_vlc_cumulative(input_df: pd.DataFrame, current_fy: str) -> pd.DataFrame:
    """Calculate Prev. VLC Cumulative."""
    df = input_df.copy(deep=False)

    # Filter (Include)
    df = df[df['Results or Targets'] == "Results"]

    # Filter (Exclude)
    df = df[~(df['FY'] == current_fy)]

    # Filter (Include)
    df = df[df['quarter'].str.contains('Q4', regex=False, na=False)]

    # Calculated Field: [Results or Targets] (update)
    utils.set_category_column(df=df, column='Results or Targets', value="Cumulative")

    # Calculated Field: [quarter] (update)
    utils.set_category_column(df=df, column='quarter', values=df['FY'].astype(str) + " Cumulative",
                              recalc_categories=True)

    # Calculated Field: [cumulative] (update)
    df.loc[:, 'cumulative'] = df['values']

    # Calculated Field: [results] (update)
    df.loc[:, 'results'] = np.NaN

    # Calculated Field: [source_name] (update)
    utils.set_category_column(df=df, column='source_name', value="Derived")

    return df


def _curr_vlc_cumulative(input_df: pd.DataFrame, current_fyq_start_date: datetime) -> pd.DataFrame:
    """Calculate Curr. VLC Cumulative."""
    df = input_df.copy(deep=False)

    # Filter (Include)
    df = df[df['quarter calendar date'] == current_fyq_start_date]

    # Calculated Field: [Results or Targets] (update)
    utils.set_category_column(df=df, column='Results or Targets', value="Cumulative")

    # Calculated Field: [quarter] (update)
    utils.set_category_column(df=df, column='quarter', values=df['FY'].astype(str) + " Cumulative",
                              recalc_categories=True)

    # Calculated Field: [cumulative] (update)
    df.loc[:, 'cumulative'] = df['values']

    # Calculated Field: [results] (update)
    df.loc[:, 'results'] = np.NaN

    # Calculated Field: [source_name] (update)
    utils.set_category_column(df=df, column='source_name', value="Derived")

    return df


def _remove_future_years(main_df: pd.DataFrame,
                         vlc_df: pd.DataFrame,
                         prev_vlc_df: pd.DataFrame,
                         curr_vlc_df: pd.DataFrame,
                         current_fyq: str) -> pd.DataFrame:
    """Merge dataframes and remove future years."""
    # Merge dataframes
    df = utils.concatenate_dfs([main_df, vlc_df, prev_vlc_df, curr_vlc_df])

    # Remove Fields
    df.drop(columns=['quarter calendar date'], inplace=True)

    # Filter (Exclude)
    df = df[~((df['quarter'].str[5] == 'Q') & (df['quarter'].str[:7] > current_fyq))]

    # Reorder columns to match flow output
    df = df.reindex(columns=_REMOVE_FUTURE_YEARS_FLOW_COL_ORDER)

    return df


def _update_output_columns(main_df: pd.DataFrame) -> pd.DataFrame:
    """Restructure dataframe to match desired ouptput"""
    # Reorder columns
    df = main_df.reindex(columns=_HYPER_ORDER_AND_POSTGRES_COL_UPDATE_MAP.keys())

    # Rename columns
    df.rename(columns=_HYPER_ORDER_AND_POSTGRES_COL_UPDATE_MAP, inplace=True)

    return df


def transform(event: Dict[str, Any]) -> pd.DataFrame:
    """Transforms MSD data files according to SI Tableau flow rules and creates transformed dataframe

    Args:
        event (Dict[str, Any]): dict of parameters
            data_version (str): To anticipate input data changes
            start_fiscal_year (int): Only process data starting in this year (discard earlier years)
            current_fiscal_year (int): Current fiscal year
            current_quarter: Current fiscal quarter
            do_output_col_update: Change order and names of output columns for e.g. postgres
            file_locators (Dict[str, Any]): Input and output file locations and encodings
                base_directory (str): Base path for all files. For S3, starts with "S3://bucketname/data_path".
                ref_table (Dict[str, str]): Dict containing path and encoding for ref table
                partner_type_table (Dict[str, str]): Dict containing path and encoding for partner_type table
                psnu_files (List[Dict[str, str]]): List of dicts containing path and encoding for multiple psnu files
                nat_subnat_file (str, str): Dict containing path and encoding for nat_subnat file

    Returns:
        pd.DataFrame: resulting dataframe

    Raises:
        ...
    """
    data_version = event[DATA_VERSION_KEY]
    start_fiscal_year = event[START_FISCAL_YEAR_KEY]
    current_fiscal_year = event[CURRENT_FISCAL_YEAR_KEY]
    current_quarter = event[CURRENT_QUARTER_KEY]
    file_locators = event[FILE_LOCATORS_KEY]
    base_directory = file_locators[BASE_DIRECTORY_KEY]

    target_fiscal_year = current_fiscal_year if current_quarter != "Q4" else current_fiscal_year + 1
    current_fy = utils.fy_from_fiscal_year(current_fiscal_year)
    current_fyq = utils.fyq_from_fiscal_year_and_quarter(current_fiscal_year, current_quarter)
    current_fyq_start_date = utils.date_from_fiscal_year_and_quarter(current_fiscal_year, current_quarter)

    # Read in Reference Table
    ref_table_df = _import_reference_table(base_directory=base_directory, ref_table=file_locators['ref_table'])
    # utils.write_csv(ref_table_df, "ref_table", compare=True)

    # Read in PartnerType Table
    partner_type_table_df = _import_partner_type_table(base_directory=base_directory,
                                                       partner_type_table=file_locators['partner_type_table'])
    # utils.write_csv(partner_type_table_df, "partner_type_table", compare=True)

    # Read in Known Issues Table
    known_issues_table_df = _import_known_issues_table(base_directory=base_directory,
                                                       known_issues_table=file_locators['known_issues_table'])
    # utils.write_csv(known_issues_table_df, "known_issues_table", compare=True)

    # Read in PSNU files
    main_df = _import_psnu_files(base_directory=base_directory, psnu_files=file_locators[PSNU_FILES_KEY])
    # utils.write_csv(main_df, "psnu", compare=True)

    # Join PSNU and Reference Table
    main_df = _sum_vs_snap(input_df=main_df, lookup_df=ref_table_df)
    del ref_table_df
    # utils.write_csv(main_df, "sum_vs_snap", compare=True)

    # Calculate Net New Targets
    net_new_df = main_df.copy(deep=True)
    net_new_df = _net_new_targets(input_df=net_new_df,
                                  start_fiscal_year=start_fiscal_year,
                                  target_fiscal_year=target_fiscal_year)
    # utils.write_csv(net_new_df, "net_new_targets", compare=True)

    # Create separate row per year
    net_new_df = _make_fy_rows(input_df=net_new_df,
                               start_fiscal_year=start_fiscal_year,
                               target_fiscal_year=target_fiscal_year)
    # utils.write_csv(net_new_df, "make_fy_rows", compare=True)

    # Create quarterly targets & running cumulative
    main_df = _qtr_targets_and_cumulative(input_df=main_df)
    # utils.write_csv(main_df, "qtr_targets", compare=True)

    # Read in Nat/Subnat file
    nat_subnat_df = _import_nat_subnat_file(base_directory=base_directory, nat_subnat_file=file_locators[NAT_SUBNAT_FILE_KEY])
    # utils.write_csv(nat_subnat_df, "nat_subnat", compare=True)

    # Create merged dataframe
    main_df = _reunite(main_df=main_df, net_new_df=net_new_df, nat_subnat_df=nat_subnat_df)
    del net_new_df, nat_subnat_df
    # utils.write_csv(main_df, "reunite", compare=True)

    # Pivot all numeric values
    main_df = _make_long(input_df=main_df)
    # utils.write_csv(main_df, "make_long", compare=True)

    # Cleaning data
    main_df = _clean_fields(input_df=main_df)
    # utils.write_csv(main_df, "clean_fields", compare=True)

    # Rename columns as per convention
    main_df = _update_field_names(input_df=main_df, current_fyq=current_fyq)
    # utils.write_csv(main_df, "update_field_names", compare=True)

    # Join Main and PartnerType Table
    main_df = _partner_type(input_df=main_df, lookup_df=partner_type_table_df)
    del partner_type_table_df
    # utils.write_csv(main_df, "partner_type", compare=True)

    # Join Main + PartnerType Table with Known Issues Table
    main_df = _known_issues(input_df=main_df, lookup_df=known_issues_table_df)
    del known_issues_table_df
    # utils.write_csv(main_df, "known_issues", compare=True)

    # Create calendar date for FY + QTR
    main_df = _create_calendar_date(input_df=main_df)
    # utils.write_csv(main_df, "create_calendar_date", compare=True)

    # Create VLC Numerators and Denominators
    vlc_df = main_df.copy(deep=True)
    vlc_df = _vlc(input_df=vlc_df)
    # utils.write_csv(vlc_df, "vlc", compare=True)

    # Pivot columns to rows
    vlc_df = _dates_to_rows(input_df=vlc_df)
    # utils.write_csv(vlc_df, "dates_to_rows", compare=True)

    # Calculate in "FYxx Qx" (fyq) format
    vlc_df = _correct_fyq(input_df=vlc_df, current_fyq_start_date=current_fyq_start_date)
    # utils.write_csv(vlc_df, "correct_fyq", compare=True)

    # Calculate Prev. VLC Cumulative
    prev_vlc_df = vlc_df.copy(deep=True)
    prev_vlc_df = _prev_vlc_cumulative(input_df=prev_vlc_df, current_fy=current_fy)
    # utils.write_csv(prev_vlc_df, "prev_vlc", compare=True)

    # Calculate Curr. VLC Cumulative
    curr_vlc_df = vlc_df.copy(deep=True)
    curr_vlc_df = _curr_vlc_cumulative(input_df=curr_vlc_df, current_fyq_start_date=current_fyq_start_date)
    # utils.write_csv(curr_vlc_df, "curr_vlc", compare=True)

    # Merge dataframes and remove future years
    main_df = \
        _remove_future_years(main_df=main_df, vlc_df=vlc_df, prev_vlc_df=prev_vlc_df, curr_vlc_df=curr_vlc_df,
                             current_fyq=current_fyq)
    del vlc_df, prev_vlc_df, curr_vlc_df
    # utils.write_csv(main_df, "remove_future", compare=True)

    # Restructure dataframe to match caller's needs
    if event[DO_OUTPUT_COL_UPDATE_KEY]:
        main_df = _update_output_columns(main_df=main_df)
        # utils.write_csv(df, "hyper_order_and_postgres", compare=False)

    return main_df
