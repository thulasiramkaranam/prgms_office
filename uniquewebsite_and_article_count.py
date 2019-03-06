

import pandas as pd
df = pd.read_excel(r'C:\Users\thulasiram.k\Downloads\2019-03-04keyword_pred.xlsx')
unique_sources  = df['Source'].unique()

dictt = {}
for i in unique_sources:
    source_data = df.loc[df['Source'] == i]
    dictt.update({i:len(source_data)})

count = sum(list(dictt.values()))
print(list(dictt.values()))
print(dictt)
print(count)
print("after count")
    
    