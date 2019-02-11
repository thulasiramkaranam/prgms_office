from bs4 import BeautifulSoup
import urllib.request as urllib2
import re
import requests
import json
import logging as lg
from openpyxl import Workbook
import re
from datetime import datetime as dt
from sklearn.feature_extraction.text import TfidfVectorizer,CountVectorizer
from nltk import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from gensim.summarization.summarizer import summarize
from gensim.summarization import keywords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
#todo iterate over the pages code is pending this is static page code
#todo implementing the loop for page or use Xpath to iterate over last 200 records

generic_elements = ['about us', 'facebook', 'privacy policy', 'newsletter', 'sitemap',
                 'feedback', 'archives', 'epaper', 'terms and conditions',
                 'advertise with us','twitter', 'google+', 'android',
                 'windows', 'windows phone', 'iphone', 'blackberry', 'ipad',
                 'advertise', 'disclaimer', 'investor', 'ombudsman',
                 'careers', 'service terms', 'terms and conditions',
                 'channel distribution', 'complaint redressal',
                    'advertising', 'events', 'subscriptions', 'group subscriptions',
                    'customer service', 'register', 'donate', 'videos', 'maps',
                    'africa', 'asia', 'europe', 'middle east', 'global commons', 'interviews',
                    'economics', 'environment', 'reviews', 'galleries', 'articles',
                    'shared', 'viewed', 'recent', 'site info', 'site news', 'terms & conditions',
                    'more about us', '<img', '<i']
generic_sub_links =['facebook.com', 'twitter.com', 'linkedin.com',
                 'play.google.com', 'itunes,apple.com','plus.google.com',
                    'pinterest.com', 'youtube.com']

content_htmltags = ['<span','<i' '<div', '<class', '<img']
page_elements = ['page', 'next']
page_elements.extend([ str(i) for i in range(100)])
articles = []
with open('dynamic_scrap.json') as json_data_file:
    json = json.load(json_data_file)

group_of_urls = set()        #creating empty set
failedwebsites = []

def write_into_csv(listt):
    analyzer= SentimentIntensityAnalyzer()
    file_name = "dynamic_scrapping"+".csv" 
    #print (listt)
    #print ("After listt")
    with open(file_name, "wb") as file_name:

        book = Workbook()
        sheet = book.active
        sheet.title = "News"
        sheet['A1'] = "Category"
        sheet['B1'] = "Datetime"
        sheet['C1'] = "URL"
        sheet['D1'] = "Headlines"
        sheet['E1'] = "Summary"
        sheet['F1'] = "Sentiment"
        sheet['G1'] = "Website"
        counter = 2
        for i in listt:
            article = []
            if type(i["summary"]) == str:
                
                sentiment = analyzer.polarity_scores(i["summary"].strip())
                sentiment = sentiment['compound'] if 'compound' in sentiment else 0
                article.extend([i["Category"], i["published_date"],i["url"], i["headline"].strip(),i["summary"].strip(), sentiment, i["Website"]])
                for m,n in enumerate(article, start = 1):
                    sheet.cell(row=counter, column=m).value = n
                counter += 1
        book.save(file_name)

def validate(url):
    response = requests.get(url)
    #print(response.status_code)
    if(response.status_code==200):
        
        return response,True
    if(response.status_code==403):
        response,type=applying(url)
        return response,type
    if(response.status_code==404):
        
        return response,False
def applying(url):
    proxies = {
        "http": 'http://103.76.50.182:8080',
        "https": 'http://1.179.189.217:8080'
    }

    response = requests.get(url, proxies=proxies)
    if(response.status_code==200):
        return response,True
    else:
        
        return response,False


def selenium_scrapping():
    print ("Inside the selenium scrapping")



def scrap_website(websitename, nextpage, websiteconfig,group_of_urls=group_of_urls):
    counter = 1
    while nextpage:
        if websiteconfig['page_link'] is not False or (websiteconfig['page_link'] is False and websiteconfig['selenium'] is False):
            
            if counter == 1 or (websiteconfig['page_link'] is False and websiteconfig['selenium'] is False):

                html_page, type = validate(websitename)
                counter += 1
            else:
                next_page = websitename + websiteconfig['page_link'].format(counter)
                html_page, type = validate(next_page)
                print ("In the else condition")
                counter += 1
                print (next_page)
                print ("After websitename")
            if type is True:
                soup = BeautifulSoup(html_page.text,"lxml")
                base_website = websiteconfig['base_website']
                for link in soup.findAll('a'):
                    #finding the valid links to hover around
                    content = link.contents
                    if len(content) > 0:
                        if len(str(content[0])) > 30 and (all(element not in str(content[0]) for element in generic_elements)):
                            url = link.get('href')
                            
                            if url is not None and (all(sublink not in url for sublink in generic_sub_links)):
                                
                                validate_link= str(url).startswith(base_website)
                                if validate_link is True or ('http' or 'https' in str(url)):
                                    if url not in group_of_urls:
                                        group_of_urls.add(url)
                                else:
                                    if url not in group_of_urls:
                                        url = base_website + str(url)
                                        group_of_urls.add(url)

               
            else:
                failedwebsites.append(websitename)
                print ("entered in else1")
                nextpage = False # Website failed with error code hence iteration not required.

        elif websiteconfig['page_link'] is False and websiteconfig['selenium'] is True:
            selenium_scrapping()
            print ("entered else2")
            nextpage = False # As it is a single page not need of iteration

        for sub_url in group_of_urls:
            sub_url_html_page, sub_url_type = validate(sub_url)
            #print ("in sub url")
            if sub_url_type is True:
                dictt = {}
                sub_url_soup = BeautifulSoup(sub_url_html_page.text,"lxml")
                headline = sub_url_soup.title.text
                summary =' '.join(map(lambda p: p.text, sub_url_soup.find_all('p')))
                dictt.update({"Website": websitename ,"Category": "Geopolitics","url": str(sub_url),
                             "headline": str(headline), "summary": summary, "published_date": "Time"})
                articles.append(dictt)
                    
                # Code to fetch summary, headline and datetime
                # If datetime is less than 30 days set nextpage = False
        nextpage = False # Currently setting it to false need to remove this when date comparasion is done
        if websiteconfig['page_link'] is False and websiteconfig['selenium'] is False:
            print ("entered else 3")
            nextpage = False  # Single page with out load more buttons and page number hence iteration not required
        print (nextpage)
        print ("After next page")    

websites=["https://www.thehindu.com/news/national"]

if __name__ == '__main__':
    for website in websites:
        scrap_website(website, True, json[website])

    write_into_csv(articles)
    print (group_of_urls)
    print ("After lines seen")                   
