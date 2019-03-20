import glob
import pandas as pd
counter=0
for files in glob.glob(r'C:\Users\thulasiram.k\Documents\scrapped_files\*'):
    if(counter==0):
        df=pd.read_excel(files,index=False)
        columns = df.columns.values.tolist()
        if 'Website' in columns:
            df = df.rename(columns = {"Website": "Source", 
                                  "Content":"Content of the News", 
                                  "Headline": "Headline of the News",
                                   "Datetime":"Date of article",
                                    "URL":"URL of News Headline"}) 
            df = df.drop(columns=['Category', 'Sentiment'])

        print(len(df))
        counter=1
    else:
        print(files)
        print("After files")
        df1=pd.read_excel(files,index=False)
        columns = df1.columns.values.tolist()
        if 'Website' in columns:

            df1 = df1.rename(columns = {"Website": "Source", 
                                  "Content":"Content of the News", 
                                  "Headline": "Headline of the News",
                                   "Datetime":"Date of article",
                                    "URL":"URL of News Headline"}) 
            df1 = df1.drop(columns=['Category', 'Sentiment'])

        df=df.append(df1)
        print(len(df))
    if len(df) > 50000:
        writer = pd.ExcelWriter(r'final.xlsx', engine='xlsxwriter',options={'strings_to_urls': False})
        df.to_excel(writer)
        