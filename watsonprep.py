import pandas as pd
import numpy as np
import codecs

import datetime as dt
from datetime import datetime

print("Start")


def clean_data(file_name):
    column_names = ["Record_ID", "CAMPNO", "MAKETXT", "MODELTXT",
                    "YEARTXT", "MFGCAMPNO", "COMPNAME", "MFGNAME",
                    "BGMAN", "ENDMAN", "RCLTYPECD", "POTAFF", "ODATE",
                    "INFLUENCED_BY", "MFGTXT", "RCDATE", "DATEA",
                    "RPNO", "FMVSS", "DESC_DEFECT", "CONEQUENCE_DEFECT",
                    "CORRECTIVE_ACTION", "NOTES", "RCL_CMPT_ID"
                    ]

    with codecs.open(file_name, "rb", encoding='utf-8', errors='ignore') as fdata:
        df = pd.read_table(fdata, names=column_names, header=None)
        print("read table")

    # converts content of column 'RCDATE' to desired timestamp format
    df['Timestamp'] = pd.to_datetime(df['RCDATE'], format='%Y%m%d', errors='ignore')

    # create a column 'Year'
    df['Year'] = df['Timestamp'].apply(lambda x: "%s" % (x.year))

    # sort dataframe by the recall year in order to have 'month/year' groups in chronological order
    df = df.sort(columns='Year')

    # create 'Month/Year' column to prepare for grouping
    df['Week/Year'] = df['Timestamp'].apply(lambda x: "%s/%s" % (x.week, x.year))

    # if column 'MAKETXT' lists 'MERCEDES' as content, rename to 'MERCEDES BENZ'
    df.ix[(df.MAKETXT == 'MERCEDES'), ['MAKETXT']] = 'MERCEDES BENZ'

    # if column 'MAKETXT' lists 'MERCEDES' as content, rename to 'MERCEDES BENZ'
    df.ix[(df.MAKETXT == 'MERCEDES-BENZ'), ['MAKETXT']] = 'MERCEDES BENZ'

    # Use when you want Audi, BMW, and Mercedes
    # We should refactor code to split the dataframe in one area. Aka the "df" should be something like ger_df
    # df=df[(df['MAKETXT'].str.contains("AUDI|BMW|MERCEDES")==True)
    #    & (df['MAKETXT'].str.contains("AUDIOVOX")==False)
    #    & (df['MAKETXT'].str.contains("JL")==False)]

    # Use when you want only Mercedes
    mb_df = df[(df['MAKETXT'].str.contains("MERCEDES BENZ") == True)
               & (df['MAKETXT'].str.contains("AUDIOVOX") == False)
               & (df['MAKETXT'].str.contains("JL") == False)]

    # Use when you only want SUVs
    # This is a dirty hodgepodge of random search strings, but had to be done for now...
    suv_df = mb_df[(df['MODELTXT'].str.contains("M CLASS|"
                                                "M-CLASS|"
                                                "ML|"
                                                "GL|"
                                                "M") == True)
                   & (df['MODELTXT'].str.contains("GLK|"
                                                  "GLC|"
                                                  "GLA|"
                                                  "MERCEDES|"
                                                  "MATS|"
                                                  "MAT|"
                                                  "BROU|"
                                                  "SMART|"
                                                  "CLA45|"
                                                  "SLR|"
                                                  "S63|"
                                                  "S65|"
                                                  "GT S|"
                                                  "METRIS") == False)]

    df = df.dropna(subset=['Timestamp', 'Year'])

    df.to_excel('output.xlsx')


if __name__ == '__main__':
    clean_data('FLAT_RCL.txt')
    print("Done")
