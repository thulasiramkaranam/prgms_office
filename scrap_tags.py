

from bs4 import BeautifulSoup
import requests
abc = 'https://indianexpress.com/article/india/amu-jnu-sedition-case-anti-india-slogans-what-is-sedition-what-isnt-5583392/'
cde = 'https://www.ndtv.com/india-news/vande-bharat-express-launch-biggest-reply-to-pulwama-attack-terrorists-says-railway-piyush-goyal-1994057'
jj = 'https://www.healtheuropa.eu/medical-cannabis-legislation/90302/'
euro = 'https://www.euronews.com/2018/10/22/italy-tells-eu-it-will-stick-to-budget-plans-and-denies-the-proposals-will-risk-eu-stabili'

brook = "https://www.brookings.edu/blog/fixgov/2019/02/11/can-immigration-reform-happen-a-look-back/"
telegraph = "https://www.telegraph.co.uk/news/2019/02/07/desperate-venezuelans-beg-maduro-accept-aid-coming-across-border/"
test = "https://nationalpost.com/news/canada/will-your-beloved-pet-eat-you-if-you-die-alone"
canada_weather = "https://globalnews.ca/news/4909707/canadas-extreme-weather-government-prepare/"
thehindu = "https://www.thehindu.com/news/national/kulbhushan-jadhav-case-pakistan-committed-to-implementing-icjs-decision-in-says-official/article26278633.ece"
africannews = "http://www.africanews.com/2019/02/14/business-boom-on-ethio-eritrea-border-amid-brisk-birr-nakfa-exchanges/"
fin24 = "https://www.fin24.com/Economy/Eskom/low-probability-of-load-shedding-for-friday-20190215"
bbc = "https://www.bbc.com/news/world-us-canada-47247726"
apnews = "https://www.apnews.com/f6b47aeabf6d407a963346b6e33252a4"
thehill = "https://thehill.com/policy/energy-environment/429502-senate-rejects-bid-to-block-future-national-monuments-in-utah"

npr = "https://www.npr.org/2019/02/14/694914509/florists-prospects-might-wilt-as-no-deal-brexit-offers-a-less-than-rosy-picture"
dailynews = "https://www.nydailynews.com/featured/ny-news-new-zealand-valentine-day-metoo-20190214-story.html"

aglmeter = "https://agmetalminer.com/2019/01/30/this-morning-in-metals-iron-ore-buyers-could-look-to-australia-after-vale-disaster/"
thehill2= "https://thehill.com/homenews/senate/430025-senate-confirms-trump-pick-william-barr-as-new-attorney-general"
metal_bultin = "https://www.metalbulletin.com/Article/3858497/Search-results/IN-CASE-YOU-MISSED-IT-5-key-stories-from-February-14.html"

canoe = "https://canoe.com/entertainment/celebrity/miranda-lambert-announces-shes-married-to-boyfriend-brendan-mcloughlin"
thelocal = "https://www.thelocal.de/20190217/germany-retakes-world-smurf-record"
economic = "https://economictimes.indiatimes.com/small-biz/startups/newsbuzz/infibeam-to-up-fintech-play-with-rs-250-cr-investment/articleshow/68041666.cms"
html_page = requests.get(economic)


soup = BeautifulSoup(html_page.text, "lxml")

bag_of_words = ['topic', 'topics', 'tag', 'tags']
not_required_words = ['instagram', 'twitter']
booln = True
links = []
article_tags = [] 
def tag_from_div(soup):
    class_string = "readanchore"
    ab = 'div'
    soup_data = soup.findAll("span", {"class": class_string})
    #tags = soup_data.findAll("a")
    print(len(soup_data))
    print("After length of data")
    if len(soup_data) > 0:
        tags = soup_data[0].findAll("a")
        for tag in tags:
            url = tag.get('href')
            contents = tag.contents
            if len(contents) > 0:
                article_tags.append(contents[0].strip())
    print(article_tags)
    return article_tags
tag_from_div(soup)
print("After calling")
for link in soup.findAll('a'):
    
    if any(x in str(link) for x in bag_of_words) and all(x not in str(link) for x in not_required_words):
        
        print(link)
        print("After link")
        links.append(link)

for l in links:
    
    clas = l.get('class')
    
    if clas is not None and any(t in clas[0] for t in bag_of_words):
        
        booln = False
        url = l.get('href')
        content = l.contents
        if len(content) > 0:
            article_tags.append(content[0].strip())
            print(content)
            print("after content")
        print("After content")
if booln is True:
    for nl in links:
        nurl = nl.get('href')
        ncontent = nl.contents
        if len(ncontent) > 0:
            article_tags.append(ncontent[0].strip())
            print(ncontent)
            print("After n content")

        

