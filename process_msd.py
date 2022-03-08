import time
#%env AWS_SECRET_ACCESS_KEY=vsdoWZK2RmTH5J7ghmDhEN7Dcnsvs4FgrHCQm8jS
import time
from datetime import datetime
import pandas as pd
from pathlib import Path
import csv
import os
import platform
import numpy as np

#pip install tableauhyperapi
from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableName,TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    HyperException
from msd_flow import (
    transform,
    DATA_VERSION_KEY,
    START_FISCAL_YEAR_KEY,
    CURRENT_FISCAL_YEAR_KEY,
    CURRENT_QUARTER_KEY,
    DO_OUTPUT_COL_UPDATE_KEY,
    FILE_LOCATORS_KEY,
    BASE_DIRECTORY_KEY,
    REF_TABLE_KEY,
    PARTNER_TYPE_TABLE_KEY,
    KNOWN_ISSUES_TABLE_KEY,
    PSNU_FILES_KEY,
    NAT_SUBNAT_FILE_KEY,
    PATH_KEY,
    ENCODING_KEY,
    NA_VALUES_KEY
)
_PLATFORM = platform.system()
print(f"Running on {_PLATFORM}")
_S3_LOCATION = "USAID" #"USAID"
#_DEBUG_BASE_DIRECTORY = r"C:\QuarterlyAnalystics\code\Kholida3\q4postcleanoutput\Debug"
# _DEBUG_BASE_DIRECTORY = r"~/Debug"
# _DEBUG_BASE_DIRECTORY = r"s3://e1-devtest-ddc-gov-usaid/ddc-quarterly-analytics/output"
_DEBUG_WRITE_ENCODING = "utf-8-sig"
#Deloitte AWS_ACCESS_KEY_ID AKIATETLQYXU5UJCAW55
#Deloitte AWS_SECRET_ACCESS_KEY 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
#USAID AWS_ACCESS_KEY_ID AKIATETLQYXU5UJCAW55
#USAID AWS_SECRET_ACCESS_KEY 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
if _S3_LOCATION == 'USAID':
    os.environ["AWS_ACCESS_KEY_ID"] = "AKIATETLQYXU5UJCAW55"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "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"
else:
    os.environ["AWS_ACCESS_KEY_ID"] = "AKIAWX6MSRPIQ2NWI6FO"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "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"

if _PLATFORM == 'Windows':
    _CSV_DATE_FORMAT = "%#m/%#d/%Y"
else:
    _CSV_DATE_FORMAT = "%-m/%-d/%Y"
    #%env AWS_ACCESS_KEY_ID=AKIAWX6MSRPIQ2NWI6FO
    #%env AWS_SECRET_ACCESS_KEY=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

chunk_size = 250000
#---------------- given a df return a list of columns with set data type
def get_data_types_from_df(df):
    colls = []

    for col_name in df.columns:
        dataTypeObj = df.dtypes[col_name]
        if dataTypeObj == np.int64:
            #print("Data type of column :" + col_name + " " + str(dataTypeObj))
            colls.append(TableDefinition.Column(col_name, SqlType.big_int(),NULLABLE))
        elif dataTypeObj == np.double:
            #print("Data type of column :" + col_name + " " +str(dataTypeObj))
            colls.append(TableDefinition.Column(col_name, SqlType.double(), NULLABLE))
        else:
            colls.append(TableDefinition.Column(col_name, SqlType.text(), NULLABLE))
    #print(colls)
    return colls

def run_create_hyper_file_from_csv_opt(connection,tablename,inputfile,hyperfile,strt):
    # Using path to current file, create a path that locates CSV file packaged with these examples.
    path_to_csv = inputfile #str(Path(__file__).parent
    #df['Mech Code'] = df['Mech Code'].values.astype(str)
    print("In  run_create_hyper_file_from_csv_opt")
    print(tablename)
    try:
        count_in_table = connection.execute_command(
            command=f"COPY {tablename} from {escape_string_literal(path_to_csv)} with "
                f"(format csv, NULL 'NULL', delimiter ',', header,FORCE_NULL)")
        print(f"The number of rows added to table {tablename} is {count_in_table}.")
    except Exception as ex:
        print("Error from run_create_hyper_file_from_csv_opt: " + str(ex))
    #print("The connection to the Hyper file has been closed.")
    #print("The Hyper process has been shut down.")

# take slice of the df using ilocator, write to csv and use hyper copy api
# repeat until end of df. This will append the contents of the csv file to the hyper file
# rationale to do this - avoid creating a large file. the temporary file can even be on the jupyter notes
def run_create_hyper_slices_df(df,incols):
    strt = 0
    end  = 0
    csvfile = "temphld.csv"
    hyperfile = "temphyper.hyper"
    try:            # delete if it is there
        os.remove(hyperfile)
        print("Deleted : " + hyperfile)
    except Exception as ex:
        print("Error from run_create_hyper_slices_df remove: " + str(ex))
    # take slice of the df using ilocator, write to csv and use hyper copy api
    # repeat until end of df. This will append the contents of the csv file to the hyper file
    # rationale to do this - avoid creating a large file. the temporary file can even be on the jupyter notes
    print("Load data from CSV into table in new Hyper file run_create_hyper_slices_df")
    path_to_database = Path(hyperfile)
    hyper_table = TableDefinition(
    # if you want only the table name, do not prefix it with an explicit schema name, in that case,the table will reside in the default "public" namespace.
        table_name= TableName('Extract','Extract'),
        columns = incols) #colls  )
    process_parameters = {
        # Limits the number of Hyper event log files to two.
        "log_file_max_count": "2",
        # Limits the size of Hyper event log files to 100 megabytes.
        "log_file_size_limit": "100M"
    }

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    hyper = HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU, parameters=process_parameters)
    connection_parameters = {"lc_time": "en_US"}
    connection = Connection(endpoint=hyper.endpoint,
            database=path_to_database,
            create_mode=CreateMode.CREATE_IF_NOT_EXISTS,#CreateMode.CREATE_AND_REPLACE,
            parameters=connection_parameters)
    try:            # delete if it is there
        connection.catalog.create_schema("Extract")
        connection.catalog.create_table(table_definition=hyper_table)
    except Exception as ex:
        print("Error from run_create_hyper_slices_df create_table: " + str(ex))
    strt = 0

    while strt < len(df):
        sdf = df.iloc[strt:strt + chunk_size]
        print(len(sdf))
        ############## replace all nas to NULL
        sdf.to_csv(csvfile, encoding='utf-8', index=False,na_rep='NULL')
        run_create_hyper_file_from_csv_opt(connection,hyper_table.table_name,csvfile,hyperfile,strt)
        strt = strt + chunk_size
    connection.close()
    hyper.close()

def run_create_hypper_using_pantab_via_pandaframe(df,hypername):
    try:
       pantab.frame_to_hyper(df, hypername, table = "Extract")
    except Exception as ex:
       print("Error run_create_hypper_using_pantab_via_pandaframe" + str(ex))
FILE_LOCATORS_ZAMBIA_LOCAL = {
    BASE_DIRECTORY_KEY: r"C:/Users/khashkes/Documents/USAID_Credence/Projects/DDC/2021 Q4",
    REF_TABLE_KEY: {
        PATH_KEY: r"Lookup Tables/MSD Ref Tables (Reporting Frequency_Summed vs. Snapshot).csv",
        ENCODING_KEY: "utf-8-sig",
        NA_VALUES_KEY: [""],
    },
    PARTNER_TYPE_TABLE_KEY: {
        PATH_KEY: r"Lookup Tables/PartnerType for Tableau.csv",
        ENCODING_KEY: "utf-8-sig",
        NA_VALUES_KEY: [""],
    },
    KNOWN_ISSUES_TABLE_KEY: {
        PATH_KEY: r"Lookup Tables/Known Data Issues Ref table.csv",
        ENCODING_KEY: "utf-8-sig",
        NA_VALUES_KEY: [""],
    },
    PSNU_FILES_KEY: [
        {
            PATH_KEY: r"Inputs/MER_Structured_Datasets_PSNU_IM_FY15-18_20211217_v2_2_Zambia.csv",
            ENCODING_KEY: "utf-8-sig",
            NA_VALUES_KEY: [""],
        },
        {
            PATH_KEY: r"Inputs/MER_Structured_Datasets_PSNU_IM_FY19-22_20211217_v2_1_Zambia.csv",
            ENCODING_KEY: "utf-8-sig",
            NA_VALUES_KEY: [""],
        },
    ],
    NAT_SUBNAT_FILE_KEY: {
        PATH_KEY: r"Inputs/MER_Structured_Datasets_NAT_SUBNAT_FY15-22_20211217_v2_1_Zambia.csv",
        ENCODING_KEY: "utf-8-sig",
        NA_VALUES_KEY: [""],
    },
}

FILE_LOCATORS_ALL_LOCAL = {
    BASE_DIRECTORY_KEY: r"C:/Users/khashkes/Documents/USAID_Credence/Projects/DDC/2021 Q4",
    REF_TABLE_KEY: {
        PATH_KEY: r"Lookup Tables/MSD Ref Tables (Reporting Frequency_Summed vs. Snapshot).csv",
        ENCODING_KEY: "utf-8-sig",
        NA_VALUES_KEY: [""],
    },
    PARTNER_TYPE_TABLE_KEY: {
        PATH_KEY: r"Lookup Tables/PartnerType for Tableau.csv",
        ENCODING_KEY: "utf-8-sig",
        NA_VALUES_KEY: [""],
    },
    KNOWN_ISSUES_TABLE_KEY: {
        PATH_KEY: r"Lookup Tables/Known Data Issues Ref table.csv",
        ENCODING_KEY: "utf-8-sig",
        NA_VALUES_KEY: [""],
    },
    PSNU_FILES_KEY: [
        {
            PATH_KEY: r"Inputs/MER_Structured_Datasets_PSNU_IM_FY15-18_20211217_v2_2.csv",
            ENCODING_KEY: "utf-8-sig",
            NA_VALUES_KEY: [""],
        },
        {
            PATH_KEY: r"Inputs/MER_Structured_Datasets_PSNU_IM_FY19-22_20211217_v2_1.csv",
            ENCODING_KEY: "utf-8-sig",
            NA_VALUES_KEY: [""],
        },
    ],
    NAT_SUBNAT_FILE_KEY: {
        PATH_KEY: r"Inputs/MER_Structured_Datasets_NAT_SUBNAT_FY15-22_20211217_v2_1.csv",
        ENCODING_KEY: "utf-8-sig",
        NA_VALUES_KEY: [""],
    },
}

FILE_LOCATORS_ZAMBIA_S3 = {
    BASE_DIRECTORY_KEY: r"s3://e1-devtest-ddc-gov-usaid",
    REF_TABLE_KEY: {
        PATH_KEY: r"ddc-quarterly-analytics/Lookup Tables/MSD Ref Tables (Reporting Frequency_Summed vs. Snapshot).csv",
        ENCODING_KEY: "utf-8-sig",
        NA_VALUES_KEY: [""],
    },
    PARTNER_TYPE_TABLE_KEY: {
        PATH_KEY: r"ddc-quarterly-analytics/Lookup Tables/PartnerType for Tableau.csv",
        ENCODING_KEY: "latin1",
        NA_VALUES_KEY: [""],
    },
    KNOWN_ISSUES_TABLE_KEY: {
        PATH_KEY: r"ddc-quarterly-analytics/Lookup Tables/Known Data Issues Tracker.csv",
        ENCODING_KEY: "utf-8-sig",
        NA_VALUES_KEY: [""],
    },
    PSNU_FILES_KEY: [
        {
            PATH_KEY: r"ddc-quarterly-analytics/q4data_zambia/MER_Structured_Datasets_PSNU_IM_FY15-18_20211217_v2_2_Zambia.csv",
            ENCODING_KEY: "utf-8-sig",
            NA_VALUES_KEY: [""],
        },
        {
            PATH_KEY: r"ddc-quarterly-analytics/q4data_zambia/MER_Structured_Datasets_PSNU_IM_FY19-22_20211217_v2_1_Zambia.csv",
            ENCODING_KEY: "utf-8-sig",
            NA_VALUES_KEY: [""],
        },
    ],
    NAT_SUBNAT_FILE_KEY: {
        PATH_KEY: r"ddc-quarterly-analytics/q4data_zambia/MER_Structured_Datasets_NAT_SUBNAT_FY15-22_20211217_v2_1_Zambia.csv",
        ENCODING_KEY: "utf-8-sig",
        NA_VALUES_KEY: [""],
    },
}

FILE_LOCATORS_LATEST_S3 = {
    #USAID
    BASE_DIRECTORY_KEY: r"s3://e1-devtest-ddc-gov-usaid/ddc-quarterly-analytics",
    #Deloitte
    #BASE_DIRECTORY_KEY: r"s3://e1-appdev-ddc-quaterly-analytics",#quarterlyinputs",
    REF_TABLE_KEY: {
        PATH_KEY: r"dev/Lookup Tables/MSD Ref Tables (Reporting Frequency_Summed vs. Snapshot).csv",
        ENCODING_KEY: "utf-8-sig",
        NA_VALUES_KEY: [""],
    },
    PARTNER_TYPE_TABLE_KEY: {
        PATH_KEY: r"dev/Lookup Tables/PartnerType for Tableau - MechID-PartnerType.csv",
        ENCODING_KEY: "utf-8-sig",
        NA_VALUES_KEY: [""],
    },
    KNOWN_ISSUES_TABLE_KEY: {
        PATH_KEY: r"dev/Lookup Tables/Known Data Issues Ref table.csv",
        ENCODING_KEY: "utf-8-sig",
        NA_VALUES_KEY: [""],
    },
    PSNU_FILES_KEY: [
        {
            PATH_KEY: r"dev/input/fy22_q1_pre_clean_data" #q4data_ou" #MSDS"
                      r"/MER_Structured_Datasets_PSNU_IM_FY15-19_20220211_v1_1.csv",
            ENCODING_KEY: "utf-8-sig",
            NA_VALUES_KEY: [""],
        },
        {
            PATH_KEY: r"dev/input/fy22_q1_pre_clean_data" #q4data_ou" #MSDS"
                      r"/MER_Structured_Datasets_PSNU_IM_FY20-22_20220211_v1_1.csv",
            ENCODING_KEY: "utf-8-sig",
            NA_VALUES_KEY: [""],
        },
    ],
    NAT_SUBNAT_FILE_KEY: {
        PATH_KEY: r"dev/input/fy22_q1_pre_clean_data" #q4data_ou" #NatSubnat"
                  r"/MER_Structured_Datasets_NAT_SUBNAT_FY15-21_20220211_v1_1.csv",
        ENCODING_KEY: "utf-8-sig",
        NA_VALUES_KEY: [""],
    },
}

if __name__ == "__main__":
    print('Starting....')
    start_counter = time.perf_counter()

    event = {
        DATA_VERSION_KEY: "1.0",
        START_FISCAL_YEAR_KEY: 2017,
        CURRENT_FISCAL_YEAR_KEY: 2022,
        CURRENT_QUARTER_KEY: "Q1",
        DO_OUTPUT_COL_UPDATE_KEY: False,
        FILE_LOCATORS_KEY: FILE_LOCATORS_LATEST_S3,
    }
    curtime=datetime.now().strftime("%m-%d %H:%M:%S")
    print("Started @ " + curtime)

    df = transform(event=event)
    curtime=datetime.now().strftime("%m-%d %H:%M:%S")
    print("Completed @ " + curtime)
    #df = pd.read_csv("C://QuarterlyAnalystics//code//data//data_part_0.csv",low_memory=False)
    #get the column data type modified based on dataframe col data type
    cols = get_data_types_from_df(df)
    try:
       if len(df) > 0:
            print(df.head())
            # take slice of the df using ilocator, write to csv and use hyper copy api
            # repeat until end of df. This will append the contents of the csv file to the hyper file
            # rationale to do this - avoid creating a large file. the temporary file can even be on the jupyter notes
            run_create_hyper_slices_df(df,cols)

    except Exception as ex:
        print("Error from main: " + str(ex))

    end_counter = time.perf_counter()
    print(f"Finished in {(end_counter - start_counter)/60:0.4f} minutes!")
