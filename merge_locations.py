



import pandas as pd
import json
import ast
import copy
df = pd.read_excel(r"C:\Users\thulasiram.k\Downloads\mvp_file.xlsx")


def ds_fmt_to_loc_match_fmt(old_dataframe):
    empty_df = pd.DataFrame()
    unique_scrape_ids = old_dataframe.Scrape_id.unique()
    for i in unique_scrape_ids:
        scrape_id_df = old_dataframe.loc[old_dataframe['Scrape_id'] == i]
        
        lat_long = scrape_id_df['Impacted_locations']
        lat_long = list(lat_long.values)[0]
        lat_long = json.loads(lat_long)
        for j in lat_long:
            individual = pd.DataFrame()
            individual = copy.deepcopy(scrape_id_df)
            individual['city_name'] = j['city']
            individual['sub_country'] = j['state']
            individual['country'] = j['Country']
            individual['Latitude'] = j['lat_long']['lat']
            individual['Longitude'] = j['lat_long']['long']
            print(individual)
            empty_df = empty_df.append(individual)
    print(len(empty_df))
    empty_df.to_excel("testing.xlsx")
    return empty_df


def loc_match_fmt_to_ds_fmt(loc_df):
    unique_scrape_ids = loc_df.Scrape_id.unique()
    for j in unique_scrape_ids:
        scrape_id_df = loc_df.loc[loc_df['Scrape_id'] == j] 
        ou_lst = []
        ou_impct_lst = []
        su_lst = []
        su_impct_lst = []
        dc_lst = []
        dc_impct_lst = []
        for i in range(len(scrape_id_df)):
            row  = scrape_id_df.iloc[i]
            ou_js = json.loads(row['impacted_OU'])
            ou_js_im = row['impact_level_on_OU']
            
            ou = {fetch_list_frm_string(str(row['impacted_OU']))[i]: fetch_list_frm_string(str(row['impact_level_on_OU']))[i] for i in range(len(fetch_list_frm_string(str(row['impacted_OU']))))}
            su = {fetch_list_frm_string(str(row['impacted_supplier']))[i]: fetch_list_frm_string(str(row['impact_level_on_supplier']))[i] for i in range(len(fetch_list_frm_string(str(row['impacted_supplier']))))}
            dc = {fetch_list_frm_string(str(row['impacted_DC']))[i]: fetch_list_frm_string(str(row['impact_level_on_DC']))[i] for i in range(len(fetch_list_frm_string(str(row['impacted_DC']))))}
            if len(ou) > 0:
                for key,val in ou.items():
                    if key not in ou_lst:
                        ou_lst.append(key)
                        ou_impct_lst.append(val)
            if len(su) > 0:
                for key,val in su.items():
                    if key not in su_lst:
                        su_lst.append(key)
                        su_impct_lst.append(val)
            if len(dc) > 0:
                for key,val in dc.items():
                    if key not in dc_lst:
                        dc_lst.append(key)
                        dc_impct_lst.append(val)
        print(ou_lst)
        print(ou_impct_lst)
        print(j)
        
        loc_df.loc[loc_df['Scrape_id'] == j, 'impacted_OU'] = str(ou_lst)
        loc_df.loc[loc_df['Scrape_id'] == j, 'impacted_supplier'] = str(su_lst)
        loc_df.loc[loc_df['Scrape_id'] == j, 'impacted_DC'] = str(dc_lst)
        loc_df.loc[loc_df['Scrape_id'] == j, 'impact_level_on_OU'] = str(ou_impct_lst)
        loc_df.loc[loc_df['Scrape_id'] == j, 'impact_level_on_supplier'] = str(su_impct_lst)
        loc_df.loc[loc_df['Scrape_id'] == j, 'impact_level_on_DC'] = str(dc_impct_lst)
    loc_df = loc_df.drop_duplicates('Scrape_id', keep='last')
    loc_df = loc_df.drop(columns=['city_name', 'sub_country', 'country', 'Latitude', 'Longitude'])
    loc_df.to_excel("final3.xlsx")


#ds_fmt_to_loc_match_fmt(old_df)       
