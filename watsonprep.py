import pandas as pd
import numpy as np
import codecs

import datetime as dt
from datetime import datetime


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
        print("[INFO] Data Table Read")

    # converts content of column 'RCDATE' to desired timestamp format
    df['Timestamp'] = pd.to_datetime(df['RCDATE'], format='%Y%m%d', errors='ignore')

    # create a column 'Year'
    df['Year'] = df['Timestamp'].apply(lambda x: "%s" % (x.year))

    # sort dataframe by the recall year in order to have 'month/year' groups in chronological order
    df = df.sort(columns='Year')

    df['Week/Year'] = df['Timestamp'].apply(lambda x: "%s/%s" % (x.week, x.year))

    df.ix[(df.MAKETXT == 'MERCEDES'), ['MAKETXT']] = 'MERCEDES BENZ'
    df.ix[(df.MAKETXT == 'MERCEDES-BENZ'), ['MAKETXT']] = 'MERCEDES BENZ'

    # Default value for big 3
    df['Big_Three'] = 'nr'
    df['Big_Three'][df['MAKETXT'].str.contains("AUDI")] = 'Audi'
    df['Big_Three'][df['MAKETXT'].str.contains("BMW")] = 'BMW'
    df['Big_Three'][df['MAKETXT'].str.contains("MERCEDES")] = 'Mercedes'

    # Cleaning up for some outliers... could be more specific in the prev. search
    df['Big_Three'][df['MAKETXT'].str.contains("AUDIOVOX")] = 'nr'
    df['Big_Three'][df['MAKETXT'].str.contains("JL")] = 'nr'

    # Isolate MBUSI cars
    df['MB_Origin'] = 'nr'
    df['MB_Origin'][df['MODELTXT'].str.contains("M CLASS|"
                                                "M-CLASS|"
                                                "ML|"
                                                "GL|"
                                                "M")] = 'MBUSI'

    df['MB_Origin'][df['MODELTXT'].str.contains("GLK|"
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
                                                "METRIS")] = 'Non-MBUSI'


    df = df.dropna(subset=['Timestamp', 'Year'])

    df.to_excel('output.xlsx')
    print('[INFO] Finished Excel write')


if __name__ == '__main__':
    clean_data('FLAT_RCL.txt')
