

import pandas as pd
from gensim.summarization.summarizer import summarize
import pandas as pd
import psycopg2


def get_keyword_map(data):
    print(data)
    keywords = list(data['Words'])
    keyword_map = dict()
    for keyword in keywords:
        split = keyword.split(',')
        category = split[1]
        keyword = split[0]

        if category in keyword_map.keys():
            keyword_map[category].append(keyword)

        else:
            keyword_map[category] = [keyword]

    for key, value in keyword_map.items():
        items = value
        words = list()
        for item in items:
            if item not in words:
                words.append(item)
        keyword_map[key] = words

    return keyword_map


def check_for_keywords(text, map):
    found = dict()
    for key, val in map.items():
        values = val
        temp = dict()

        for value in values:
            result = text.find(value)
            if result is not -1:
                if value in temp.keys():
                    temp[value] += 1
                else:
                    temp[value] = 1

        if len(temp.keys()) == 0:
           found[key] = 0
        else:
            found[key] = list(temp.keys())
    return found



def main(data):
    print(data)
    conn = psycopg2.connect(host="neo-app-vso.c8yss8lfzn7r.us-east-1.rds.amazonaws.com" ,port=5432,dbname='senseai',user="neo_app_vso_root",password="Bcone,12345")
    cur = conn.cursor()

    keyword_data = pd.read_excel('/home/ec2-user/linuxcode/framework/data_files/Alert 101.xlsx')
    key_map = get_keyword_map(data=keyword_data)
    print(keyword_data)
    output = pd.DataFrame(
        columns=["Scrape_id", "Date of article", "Source", "URL of News Headline", "Headline of the News",
                 "Content of the News", "Tags", "Summary", "Severity", "Feature", "Class Level1", "Probability1",
                 "Class Level2", "Probability2", "Event Date", "city_name", "sub_country", "country", "Latitude",
                 "Longitude", 'Metals', 'Precious Metals',
                 'Currency', 'Energy', 'Polymers', 'Agriculture Produce',
                 'Keyword Found'])

    for index, row in data.iterrows():
        headline = row['Headline of the News']
        content = row['Content of the News']
        summary = summarize(text=content, ratio=0.5)
        id_1 = row['Scrape_id']
        DOA = row['Date of article']
        SRC = row['Source']
        URL = row['URL of News Headline']
        tags = row['Tags']
        sev = row['Severity']
        feature = row['Feature']
        cl1 = row['Class Level1']
        pl1 = row['Probability1']
        cl2 = row['Class Level2']
        pl2 = row['Probability2']
        dte = row['Event Date']
        city = row['city_name']
        state = row['sub_country']
        country = row['country']
        lat = row['Latitude']
        long1 = row['Longitude']
        if len(tags) == 1:
            corpus = headline + content + content
        else:
            corpus = headline + content + summary + tags
        keywords = check_for_keywords(text=corpus, map=key_map)

        metals = keywords['Metals']
        prec = keywords['Precious metals']
        currency = keywords['Currency']
        energy = keywords['Energy']
        polymers = keywords['Polymers']
        agriculture = keywords['Agriculture Produce']

        if (metals == 0 and prec == 0 and currency == 0 and \
                energy == 0 and energy == 0 and polymers == 0 and agriculture == 0):
            found = 'No'
        else:
            found = 'Yes'

        output = output.append({'Scrape_id': id_1, 'Date of article': DOA, 'Source': SRC, 'URL of News Headline': URL,
                                'Content of the News': content, 'Tags': tags, 'Severity': sev, 'Feature': feature,
                                'Class Level1': cl1, 'Probability1': pl1, 'Class Level2': cl2, 'Probability2': pl2,
                                'Event Date': dte, 'city_name': city, 'sub_country': state, 'country': country,
                                'Latitude': lat, 'Longitude': long1, 'Summary': summary, 'Metals': metals,
                                'Precious Metals': prec, 'Currency': currency, 'Energy': energy,
                                'Polymers': polymers, 'Agriculture Produce': agriculture, 'Keyword Found': found,'Headline of the News':headline},
                               ignore_index=True)
    print(key_map)
    writer = pd.ExcelWriter('/home/ec2-user/linuxcode/framework/data_files/KeywordSearchOutput.xlsx')
    output.to_excel(excel_writer=writer, sheet_name='Sheet1')
    writer.save()
    data=pd.read_excel(r'/home/ec2-user/linuxcode/framework/data_files/KeywordSearchOutput.xlsx')
    data=data[data['Keyword Found']=='Yes']
    print(data['Headline of the News'])
    return data


if __name__ == '__main__':

    data = pd.read_excel('/home/ec2-user/linuxcode/framework/data_files/2019-02-24.xlsx')
    main(data)