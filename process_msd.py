import time

from msd_flow import (
    transform,
    DATA_VERSION_KEY,
    START_FISCAL_YEAR_KEY,
    CURRENT_FISCAL_YEAR_KEY,
    CURRENT_QUARTER_KEY,
    FILE_LOCATORS_KEY,
    BASE_DIRECTORY_KEY,
    REF_TABLE_KEY,
    PARTNER_TYPE_TABLE_KEY,
    KNOWN_ISSUES_TABLE_KEY,
    PSNU_FILES_KEY,
    NAT_SUBNAT_FILE_KEY,
    PATH_KEY,
    ENCODING_KEY,
)

FILE_LOCATORS_ZAMBIA_LOCAL = {
    BASE_DIRECTORY_KEY: r"C:/Users/khashkes/Documents/USAID_Credence/Projects/DDC/2021 Q4",
    REF_TABLE_KEY: {
        PATH_KEY: r"Lookup Tables/MSD Ref Tables (Reporting Frequency_Summed vs. Snapshot).csv",
        ENCODING_KEY: "utf-8-sig",
    },
    PARTNER_TYPE_TABLE_KEY: {
        PATH_KEY: r"Lookup Tables/PartnerType for Tableau - MechID-PartnerType.csv",
        ENCODING_KEY: "latin1",
    },
    KNOWN_ISSUES_TABLE_KEY: {
        PATH_KEY: r"Lookup Tables/Known Data Issues Ref table.csv",
        ENCODING_KEY: "utf-8-sig",
    },
    PSNU_FILES_KEY: [
        {
            PATH_KEY: r"MER_Structured_Datasets_PSNU_IM_FY15-18_20210917_v2_1_Zambia.csv",
            ENCODING_KEY: "latin1",
        },
        {
            PATH_KEY: r"MER_Structured_Datasets_PSNU_IM_FY19-22_20211112_v1_1_Zambia_KK.csv",
            ENCODING_KEY: "utf-8-sig",
        },
    ],
    NAT_SUBNAT_FILE_KEY: {
        PATH_KEY: r"MER_Structured_Datasets_NAT_SUBNAT_FY15-22_20211112_v1_1_Zambia.csv",
        ENCODING_KEY: "latin1",
    },
}

FILE_LOCATORS_ZAMBIA_S3 = {
    BASE_DIRECTORY_KEY: r"s3://e1-appdev-ddc-quaterly-analytics",
    REF_TABLE_KEY: {
        PATH_KEY: r"Lookup Tables/MSD Ref Tables (Reporting Frequency_Summed vs. Snapshot).csv",
        ENCODING_KEY: "latin1",
    },
    PARTNER_TYPE_TABLE_KEY: {
        PATH_KEY: r"Lookup Tables/PartnerType for Tableau - MechID-PartnerType.csv",
        ENCODING_KEY: "latin1",
    },
    KNOWN_ISSUES_TABLE_KEY: {
        PATH_KEY: r"Lookup Tables/Known Data Issues Ref table.csv",
        ENCODING_KEY: "latin1",
    },
    PSNU_FILES_KEY: [
        {
            PATH_KEY: r"q4data/q4data_zambia/MER_Structured_Datasets_PSNU_IM_FY15-18_20210917_v2_1_Zambia.csv",
            ENCODING_KEY: "latin1",
        },
        {
            PATH_KEY: r"q4data/q4data_zambia/MER_Structured_Datasets_NAT_SUBNAT_FY15-22_20211112_v1_1_Zambia.csv",
            ENCODING_KEY: "utf-8-sig",
        },
    ],
    NAT_SUBNAT_FILE_KEY: {
        PATH_KEY: r"q4data/q4data_zambia/MER_Structured_Datasets_PSNU_IM_FY19-22_20211112_v1_1_Zambia.csv",
        ENCODING_KEY: "latin1",
    },
}

FILE_LOCATORS_LATEST_S3 = {
    BASE_DIRECTORY_KEY: r"s3://e1-appdev-ddc-quaterly-analytics",
    REF_TABLE_KEY: {
        PATH_KEY: r"MSD Ref Tables (Reporting Frequency_Summed vs. Snapshot).csv",
        ENCODING_KEY: "latin1",
    },
    PARTNER_TYPE_TABLE_KEY: {
        PATH_KEY: r"Lookup Tables/PartnerType for Tableau - MechID-PartnerType.csv",
        ENCODING_KEY: "latin1",
    },
    KNOWN_ISSUES_TABLE_KEY: {
        PATH_KEY: r"Lookup Tables/Known Data Issues Ref table.csv",
        ENCODING_KEY: "utf-8-sig",
    },
    PSNU_FILES_KEY: [
        {
            PATH_KEY: r"q4data/q4data_ou/MER_Structured_Datasets_PSNU_IM_FY15_18_20210917_v2_1.csv",
            ENCODING_KEY: "utf-8-sig",
        },
        {
            PATH_KEY: r"q4data/q4data_ou/MER_Structured_Datasets_PSNU_IM_FY19-22_20211112_v1_1.csv",
            ENCODING_KEY: "utf-8-sig",
        },
    ],
    NAT_SUBNAT_FILE_KEY: {
        PATH_KEY: r"q4data/q4data_ou/MER_Structured_Datasets_NAT_SUBNAT_FY15-22_20211112_v1_1.csv",
        ENCODING_KEY: "utf-8-sig",
    },
}

if __name__ == "__main__":
    print('Starting....')
    start_counter = time.perf_counter()

    event = {
        DATA_VERSION_KEY: "1.0",
        START_FISCAL_YEAR_KEY: 2017,
        CURRENT_FISCAL_YEAR_KEY: 2021,
        CURRENT_QUARTER_KEY: "Q4",
        FILE_LOCATORS_KEY: FILE_LOCATORS_ZAMBIA_LOCAL,
    }

    transform(event=event)

    end_counter = time.perf_counter()
    print(f"Finished in {end_counter - start_counter:0.4f} seconds!")
