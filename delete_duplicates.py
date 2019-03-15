
import pandas as pd
import os
import datetime
from datetime import datetime as dtt
from datetime import timedelta
def remove_duplicates(df):
    old_df = pd.DataFrame()
    for i in range(1,8):
        old_file = str(dtt.now().date() - timedelta(days=i))+'.xlsx'
    
        old_file_path = "C:\\Users\\thulasiram.k\\prgms_office\\" + old_file
    
        if os.path.isfile(old_file_path) is True:
            
            old_file = pd.read_excel(old_file_path)
            old_file = old_file.rename(columns = {'Headline of the News': 'Headline'})
            old_df = old_df.append(old_file)
    df = df.rename(columns = {'Headline of the News': 'Headline'})
    common = old_df.merge(df, on=['Headline'])
    new_df = df[(~df.Headline.isin(common.Headline))]
    new_df = new_df.rename(columns = {'Headline': 'Headline of the News'})
    return new_df
new_excel = pd.read_excel("C:\\Users\\thulasiram.k\\prgms_office\\2019-03-14.xlsx")
output = remove_duplicates(new_excel)
output.to_excel("final.xlsx")

int((dtt.strptime(str(row['Event Date'])[:19], "%Y-%m-%d %H:%M:%S")-datetime.datetime(1970,1,1)).total_seconds())
