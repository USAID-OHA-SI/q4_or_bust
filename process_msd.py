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
    NA_VALUES_KEY
)

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
            PATH_KEY: r"ddc-quarterly-analytics/q4data_ou/MER_Structured_Datasets_PSNU_IM_FY15-18_20211217_v2_2.csv",
            ENCODING_KEY: "utf-8-sig",
            NA_VALUES_KEY: [""],
        },
        {
            PATH_KEY: r"ddc-quarterly-analytics/q4data_ou/MER_Structured_Datasets_PSNU_IM_FY19-22_20211217_v2_1.csv",
            ENCODING_KEY: "utf-8-sig",
            NA_VALUES_KEY: [""],
        },
    ],
    NAT_SUBNAT_FILE_KEY: {
        PATH_KEY: r"ddc-quarterly-analytics/q4data_ou/MER_Structured_Datasets_NAT_SUBNAT_FY15-22_20211217_v2_1.csv",
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
        CURRENT_FISCAL_YEAR_KEY: 2021,
        CURRENT_QUARTER_KEY: "Q4",
        FILE_LOCATORS_KEY: FILE_LOCATORS_ALL_LOCAL,
    }

    df = transform(event=event)

    end_counter = time.perf_counter()
    print(f"Finished in {end_counter - start_counter:0.4f} seconds!")
    if len(df) > 0:
        print(df.head())
        df.to_csv("processedout.csv", encoding='utf-8', index=False)
