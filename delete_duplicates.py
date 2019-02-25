
import pandas as pd
import os
import datetime
from datetime import datetime as dtt
from datetime import timedelta
def remove_duplicates(df):
    old_file = str(dtt.now().date() - timedelta(days=1))+'.xlsx'
    if os.path.isfile(r'C:\Users\thulasiram.k\prgms_office\test1.xlsx') is True:
        old_file = pd.read_excel(r'C:\Users\thulasiram.k\prgms_office\test1.xlsx')
        appended_file = df.append(old_file, ignore_index = True)
        new_df = appended_file.drop_duplicates(['Headline of the News'])
        return new_df
    elif os.path.isfile(r'C:\Users\thulasiram.k\prgms_office\test1.xlsx') is True:
        old_file = pd.read_excel(r'C:\Users\thulasiram.k\prgms_office\test1.xlsx')
        appended_file = df.append(old_file, ignore_index = True)
        new_df = appended_file.drop_duplicates(['Headline of the News'])
        return new_df
    else:
        return df

