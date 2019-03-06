

import pandas as pd
def test():
    df = pd.read_excel(r'C:\Users\thulasiram.k\prgms_office\droped.xlsx')
    print(len(df))
    df.drop_duplicates(inplace = True)  
    return df
ee = test()
print(len(ee))
print(ee)
print(type(ee))
#ee.to_excel("droped2.xlsx")

