
import requests
from selenium import webdriver
from bs4 import BeautifulSoup
from datetime import datetime as dtt
from gensim.summarization.summarizer import summarize
def get_only_text(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.text, "lxml")
    text = ' '.join(map(lambda p: p.text, soup.find_all('p')))
    return(text)



driver = webdriver.PhantomJS('C:\\Users\\thulasiram.k\\Downloads\\phantomjs-2.1.1-windows\\phantomjs-2.1.1-windows\\bin\\phantomjs')
url = "https://news.samsung.com/global/"
driver.get(url)
html = driver.page_source.encode('utf-8')
page_num = 1
while True:

    try:
        driver.find_element_by_link_text(str(page_num)).click()
    except:
        pass
    page_num += 1
    if page_num > 3:
        break
html = driver.page_source.encode('utf-8')
data = BeautifulSoup(html, 'lxml')
headlines = data.findAll("div",{'class':'desc'})
for head in headlines:
    
    headline = head.text
    date = head.findAll('span', {'class': 'date'})
    date =  dtt.strptime(date[0].text,  "%B %d, %Y")
    url = head.findAll('a')[0]['href']
    text_data = get_only_text(url)
    final_summary = summarize(text_data)
    print ("--------------------------------------------")
    print (final_summary)
    print ("After final summary")
    break


# f = open ('firstsamsungpage.txt', 'w')
# f.write(str(html))
# f.close()

# pagenum = 0
# #f = open ('samsung_globaldata2.txt', 'w', encoding = 'utf-8')

# #driver.find_elements_by_css_selector('on').click()

# for i in range(2,5):
#     driver.
#     driver.find_element_by_name(i).click()
#     #url = '/html/body/div[1]/div[2]/div[2]/div[1]/div[2]/div[1]/div/ul/li['+str(i)+']/a'
#     #driver.find_element_by_xpath(xpath = url).click()
    
# html2 = driver.page_source.encode('utf-8')
# f = open('secondsamsung.txt', 'w')
# f.write(str(html2))
# f.close()



