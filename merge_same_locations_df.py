


import json
import pandas as pd
df = pd.read_excel(r"C:\Users\thulasiram.k\Downloads\2019-03-13refresh.xlsx")
def merge_locations(df):
        
    unique_rows = df['Scrape_id'].unique()
    for i in unique_rows:
        scrapid_rows = df.loc[df['Scrape_id'] == i]
        impacted_locations = []
        for j in range(len(scrapid_rows)):
            dictt = {}
            row = scrapid_rows.iloc[j]
            dictt.update({"city": row["city_name"], "Country": row["country"], "state": row["sub_country"],
                    "lat_long":{"lat":row["Latitude"], "long": row["Longitude"]} })
            impacted_locations.append(dictt)
        
        df.loc[df['Scrape_id'] == i, 'Impacted_locations'] = json.dumps(impacted_locations)
    return df
output = merge_locations(df)
output.to_excel("testing.xlsx")

 