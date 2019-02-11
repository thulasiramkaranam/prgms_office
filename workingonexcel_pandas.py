import pandas as pd

df=pd.ExcelFile(r'C:\Users\thulasiram.k\Downloads\PWS_Requirements_MVP_31 OCT_aman.xlsx')
data_source=pd.read_excel(df,sheet_name='Data Sources')


if data_source.Comment == 'Able to scrap seemlesly':
    print("in the if")
for i in data_source:
    print(i)
    print(type(i)
          )
    


