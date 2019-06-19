
import json
import re
cd = {
  "ResultSet": {
    "Rows": [
      {
        "Data": [
          {
            "VarCharValue": "event_id"
          },
          {
            "VarCharValue": "article_url"
          },
          {
            "VarCharValue": "scrape_id"
          },
          {
            "VarCharValue": "article_source"
          },
          {
            "VarCharValue": "class1"
          },
          {
            "VarCharValue": "class2"
          },
          {
            "VarCharValue": "content"
          },
          {
            "VarCharValue": "epoch_time"
          },
          {
            "VarCharValue": "event_date"
          },
          {
            "VarCharValue": "event_epoch_time"
          },
          {
            "VarCharValue": "feature"
          },
          {
            "VarCharValue": "headline"
          },
          {
            "VarCharValue": "impacted_locations"
          },
          {
            "VarCharValue": "probability1"
          },
          {
            "VarCharValue": "probability2"
          },
          {
            "VarCharValue": "publication_date"
          },
          {
            "VarCharValue": "severity"
          },
          {
            "VarCharValue": "summary"
          },
          {
            "VarCharValue": "tags"
          },
          {
            "VarCharValue": "headline_original"
          }
        ]
      },
      {
        "Data": [
          {
            "VarCharValue": "DR100143"
          },
          {
            "VarCharValue": "https://wallmine.com/news/1hc58b/westjet-facing-flight-delays-cancellations-from-disruptions-at-u-s-airports"
          },
          {
            "VarCharValue": "5"
          },
          {
            "VarCharValue": "https://wallmine.com/news/market/us"
          },
          {
            "VarCharValue": "Disruptive"
          },
          {
            "VarCharValue": "Port Disruption"
          },
          {
            "VarCharValue": "WestJet Airlines, CanadaÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢s No. 2 carrier, is experiencing delayed and canceled flights and expects ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œfurther delays and possible cancellations if more constraints are placedÃƒÂ¢Ã¢â€šÂ¬Ã‚Â\u009d on the U.S. air traffic system, a spokeswoman said on Friday.\r\r\r\r\r\r\n\r\r\r\r\r\r\n\r\r\r\r\r\r\n\r\r\r\r\r\r\n   Hundreds of flights were grounded or delayed at New York-area and Philadelphia airports as more air traffic controllers called in sick on Friday, in one of the most tangible signs yet of disruption from a 35-day partial shutdown of the U.S. government. ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œWe are responding to the situation as required to keep our guests moving,ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â\u009d said WestJet spokeswoman Lauren Stewart by email. Published at 25 January 2019 at 3:48pm EST on Reuters. Give wallmine a try ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Å“ it's free. \r\r\r\r\r\r\nEnglish (US)\r\r\r\r\r\r\nEnglish (United Kingdom)\r\r\r\r\r\r\nEnglish (Canada)\r\r\r\r\r\r\nEnglish (India)\r\r\r\r\r\r\nEnglish (Australia)\r\r\r\r\r\r\nEnglish (New Zeland)\r\r\r\r\r\r\nSlovensky\r\r\r\r\r\r\nFranÃƒÆ’Ã‚Â§ais\r\r\r\r\r\r\nÃƒÂ¦Ã¢â‚¬â€\u009dÃ‚Â¥ÃƒÂ¦Ã…â€œÃ‚Â¬ÃƒÂ¨Ã‚ÂªÃ…Â¾\r\r\r\r\r\r\nDeutsch\r\r\r\r\r\r\nEspaÃƒÆ’Ã‚Â±ol\r\r\r\r\r\r\nPolskie"
          },
          {
            "VarCharValue": "1552047814"
          },
          {
            "VarCharValue": "25-01-2019 00:00"
          },
          {
            "VarCharValue": "1548374400"
          },
          {
            "VarCharValue": "westjet facing flight delay cancellation disruption u s airport wallminepublished 25 january 2019 3 48pm est reuterswestjet facing flight delay cancellation disruption u s airportswestjet airline canada s no 2 carrier experiencing delayed canceled flight expects further delay possible cancellation constraint placed u s air traffic system spokeswoman said friday hundred flight grounded delayed new york area philadelphia airport air traffic controller called sick friday one tangible sign yet disruption 35 day partial shutdown u s government we responding situation required keep guest moving said westjet spokeswoman lauren stewart email"
          },
          {
            "VarCharValue": "Port Disruption at United States"
          },
          {
            "VarCharValue": "[  { \"M\" : {      \"city\" : { \"S\" : \"none\" },      \"Country\" : { \"S\" : \"United States\" },      \"lat_long\" : { \"M\" : {          \"lat\" : { \"S\" : \"37.09\" },          \"long\" : { \"S\" : \"-95.712\" }        }      },      \"state\" : { \"S\" : \"none\" }    }  }]"
          },
          {
            "VarCharValue": "0.486465537"
          },
          {
            "VarCharValue": "0.84853345"
          },
          {
            "VarCharValue": "25-01-2019 00:00"
          },
          {
            "VarCharValue": "6"
          },
          {
            "VarCharValue": "2 carrier, is experiencing delayed and canceled flights and expects Ã¢â‚¬Å“further delays and possible cancellations if more constraints are placedÃ¢â‚¬Â\u009d on the U.S. air traffic system, a spokeswoman said on Friday. Hundreds of flights were grounded or delayed at New York-area and Philadelphia airports as more air traffic controllers called in sick on Friday, in one of the most tangible signs yet of disruption from a 35-day partial shutdown of the U.S. government."
          },
          {
            "VarCharValue": "[  { \"S\" : \"spokeswoman\" },  { \"S\" : \"traffic\" },  { \"S\" : \"tangible\" },  { \"S\" : \"facing\" },  { \"S\" : \"airport\" },  { \"S\" : \"delay\" },  { \"S\" : \"cancellation\" },  { \"S\" : \"flight\" },  { \"S\" : \"air\" },  { \"S\" : \"disruption\" }]"
          },
          {
            "VarCharValue": "WestJet facing flight delays, cancellations, from disruptions at U.S. airports"
          }
        ]
      },
      {
        "Data": [
          {
            "VarCharValue": "DR100161"
          },
          {
            "VarCharValue": "https://www.foxnews.com/health/measles-outbreak-spurs-vaccination-surge-in-anti-vaxxer-hotspot"
          },
          {
            "VarCharValue": "123"
          },
          {
            "VarCharValue": "https://www.foxnews.com/health"
          },
          {
            "VarCharValue": "Disruptive"
          },
          {
            "VarCharValue": "Human Health"
          },
          {
            "VarCharValue": "This material may not be published, broadcast, rewritten,      or redistributed. Ãƒâ€šÃ‚Â©2019 FOX News Network, LLC. All rights reserved.      All market data delayed 20 minutes.     Ethan Lindenberger gets first vaccine as Washington state struggles with a measles outbreak. Weeks after a hotspot for anti-vaxxers turned into a hotspot for measles infections, vaccination rates have surged in the area, according to news reports. Last month, following 50 confirmed cases and 11 suspected cases of the measles, Clark County, Washington, declared a public health emergency. Now, residents of the area are scrambling for vaccinations, according toÃƒâ€šÃ‚Â Kaiser Health News. HEALTH OFFICIALS IN TEXAS CONFIRM AT LEAST 5 CASES OF MEASLES Compared with January of last year, measles vaccinations in Clark County are up 500 percent, from 530 doses to 3,150 doses. Statewide, the number of measles vaccinations increased by about 30 percent, from 12,140 doses last January to 15,780 this January, Kaiser Health News reported. [27 Devastating Infectious Diseases] The measles virus isÃƒâ€šÃ‚Â extremely contagiousÃƒâ€šÃ‚Â but is also considered \"extremely rare,\" because it's easily preventable with vaccines. But an increase in anti-vaccination movements across the country and even inÃƒâ€šÃ‚Â other parts of the worldÃƒâ€šÃ‚Â has left children unprotected and vulnerable to the infection. The outbreak in Washington state is one of three current measles outbreaks in the U.S. There are also outbreaks in New York City and New York state. NORTHWEST MEASLES OUTBREAK PROMPTS LOOK AT VACCINE EXEMPTIONSÃƒâ€šÃ‚Â  The MMR vaccine protects against three different viruses: measles, mumps and rubella. (There's also a different form of the vaccine, called the MMRV vaccine, that protects against those three diseases plus varicella, which is the virus that causes chickenpox.) Children should be given two doses of the MMR vaccine. The first should be administered when the child is from 12 to 15 months of age and the second when the child is from 4 to 6 years of age, according to theÃƒâ€šÃ‚Â Centers for Disease Control and Prevention. If a child receives one dose of the vaccine, he or she is protected from the infection 93 percent of the time. With two doses, a child is protected 97 percent of the time, according to the CDC.Ãƒâ€šÃ‚Â Adults who haven't been vaccinatedÃƒâ€šÃ‚Â should get at least one dose of the MMR vaccine, according to the CDC. CLICK HERE TO GET THE FOX NEWS APP Once the vaccine is administered, it takes about 72 hours to confer protection. Tiny & Nasty: Images of Things That Make Us Sick 6 Superbugs to Watch Out For 10 Deadly Diseases That Hopped Across Species Originally published onÃƒâ€šÃ‚Â Live Science. This material may not be published, broadcast, rewritten, or redistributed. Ãƒâ€šÃ‚Â©2019 FOX News Network, LLC. All rights reserved. All market data delayed 20 minutes."
          },
          {
            "VarCharValue": "1552047814"
          },
          {
            "VarCharValue": "11-02-2019 00:00"
          },
          {
            "VarCharValue": "1549843200"
          },
          {
            "VarCharValue": "measles outbreak spur vaccination surge anti vaxxer hotspot fox newsthis material may published broadcast rewritten week hotspot anti vaxxers turned hotspot measles infection vaccination rate surged area according news report last month following 50 confirmed case 11 suspected case measles clark county washington declared public health emergency now resident area scrambling vaccination according kaiser health news health official texas confirm least 5 case measles compared january last year measles vaccination clark county 500 percent 530 dos 3 150 dos statewide number measles vaccination increased 30 percent 12 140 dos last january 15 780 january kaiser health news reported child given two dos mmr vaccine child receives one dose vaccine protected infection 93 percent time two dos child protected 97 percent time according cdc material may published broadcast rewritten redistributed"
          },
          {
            "VarCharValue": "Measles outbreak spurs vaccination surge in anti-vaxxer hotspot"
          },
          {
            "VarCharValue": "[  { \"M\" : {      \"city\" : { \"S\" : \"Clark County\" },      \"Country\" : { \"S\" : \"Unites States\" },      \"lat_long\" : { \"M\" : {          \"lat\" : { \"S\" : \"45.746\" },          \"long\" : { \"S\" : \"-122.519\" }        }      },      \"state\" : { \"S\" : \"Washington\" }    }  }]"
          },
          {
            "VarCharValue": "0.512811466"
          },
          {
            "VarCharValue": "0.973140289"
          },
          {
            "VarCharValue": "11-02-2019 00:00"
          },
          {
            "VarCharValue": "7"
          },
          {
            "VarCharValue": "Weeks after a hotspot for anti-vaxxers turned into a hotspot for measles infections, vaccination rates have surged in the area, according to news reports.\r\nLast month, following 50 confirmed cases and 11 suspected cases of the measles, Clark County, Washington, declared a public health emergency.\r\nNow, residents of the area are scrambling for vaccinations, according to Kaiser Health News.\r\nNORTHWEST MEASLES OUTBREAK PROMPTS LOOK AT VACCINE EXEMPTIONSThe MMR vaccine protects against three different viruses: measles, mumps and rubella.\r\nAdults who haven't been vaccinated should get at least one dose of the MMR vaccine, according to the CDC."
          },
          {
            "VarCharValue": "[  { \"S\" : \"surge\" },  { \"S\" : \"child\" },  { \"S\" : \"antivaxxer\" },  { \"S\" : \"measles\" },  { \"S\" : \"vaccine\" },  { \"S\" : \"york\" },  { \"S\" : \"hotspot\" },  { \"S\" : \"mmr\" },  { \"S\" : \"vaccination\" },  { \"S\" : \"case\" },  { \"S\" : \"outbreak\" },  { \"S\" : \"health\" }]"
          },
          {
            "VarCharValue": "Measles outbreak spurs vaccination surge in anti-vaxxer hotspot"
          }
        ]
      },
      {
        "Data": [
          {
            "VarCharValue": "2"
          },
          {
            "VarCharValue": "https://www.sanews.gov.za/south-africa/six-die-gloria-mine-explosion"
          },
          {
            "VarCharValue": "124"
          },
          {
            "VarCharValue": "https://www.sanews.gov.za/"
          },
          {
            "VarCharValue": "Disruptive"
          },
          {
            "VarCharValue": "Explosion"
          },
          {
            "VarCharValue": "Six people have been reported dead following an explosion at the Gloria Mine in Mpumalanga. ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œThe area has since been declared a crime scene and cordoned off due to the volatility of the situation. All role-players are currently at the scene, including the Mine Rescue Services,ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â\u009d said the Department of Mineral Resources in a tweet on Thursday. On Wednesday, it was reported that an explosion took place at the mine located in Blinkpan, leading to the ground caving in, at a shaft entered by illegal miners. Police in Mpumalanga have since opened an inquest docket after five bodies were retrieved with others still being trapped inside the shaft. The department has deployed inspectors to the scene, while also engaging with Mine Rescue Services for assistance. Ãƒâ€šÃ‚Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Å“ SAnews.gov.za Follow SAnews on Facebook Media are welcome to utilise all stories, pictures and other material on this site as well as from our Facebook and Twitter accounts, at no cost. General Enquiries newsfiles@gcis.gov.za Tel: 012 473 0213 Editor Roze Moodley roze@gcis.gov.za News Editor Janine Arcangeli janine@gcis.gov.za  Ãƒâ€šÃ‚Â© 2019Ãƒâ€šÃ‚Â Government Communication and Information System"
          },
          {
            "VarCharValue": "1552047814"
          },
          {
            "VarCharValue": "06-02-2019 00:00"
          },
          {
            "VarCharValue": "1549411200"
          },
          {
            "VarCharValue": "six die gloria mine explosion sanewssix people reported dead following explosion gloria mine mpumalanga role player currently scene including mine rescue service said department mineral resource tweet thursday department deployed inspector scene also engaging mine rescue service assistance"
          },
          {
            "VarCharValue": "Explosion at Mpumalanga"
          },
          {
            "VarCharValue": "[  { \"M\" : {      \"city\" : { \"S\" : \"none\" },      \"Country\" : { \"S\" : \"South Africa\" },      \"lat_long\" : { \"M\" : {          \"lat\" : { \"S\" : \"-25.565\" },          \"long\" : { \"S\" : \"30.527\" }        }      },      \"state\" : { \"S\" : \"Mpumalanga\" }    }  }]"
          },
          {
            "VarCharValue": "0.607366591"
          },
          {
            "VarCharValue": "0.887172648"
          },
          {
            "VarCharValue": "07-02-2019 00:00"
          },
          {
            "VarCharValue": "6"
          },
          {
            "VarCharValue": "Six people have been reported dead following an explosion at the Gloria Mine in Mpumalanga.\r\nÃ¢â‚¬Å“The area has since been declared a crime scene and cordoned off due to the volatility of the situation.\r\nAll role-players are currently at the scene, including the Mine Rescue Services,Ã¢â‚¬Â\u009d said the Department of Mineral Resources in a tweet on Thursday.\r\nOn Wednesday, it was reported that an explosion took place at the mine located in Blinkpan, leading to the ground caving in, at a shaft entered by illegal miners.\r\nThe department has deployed inspectors to the scene, while also engaging with Mine Rescue Services for assistance."
          },
          {
            "VarCharValue": "[  { \"S\" : \"rescue\" },  { \"S\" : \"service\" },  { \"S\" : \"gloria\" },  { \"S\" : \"explosion\" },  { \"S\" : \"scene\" },  { \"S\" : \"volatility\" },  { \"S\" : \"thursdayon\" },  { \"S\" : \"department\" }]"
          },
          {
            "VarCharValue": "Six die in Gloria Mine explosion"
          }
        ]
      }
    ],
    "ResultSetMetadata": {
      "ColumnInfo": [
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "event_id",
          "Label": "event_id",
          "Type": "varchar",
          "Precision": 2147483647,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": True
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "article_url",
          "Label": "article_url",
          "Type": "varchar",
          "Precision": 2147483647,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": True
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "scrape_id",
          "Label": "scrape_id",
          "Type": "integer",
          "Precision": 10,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": False
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "article_source",
          "Label": "article_source",
          "Type": "varchar",
          "Precision": 2147483647,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": True
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "class1",
          "Label": "class1",
          "Type": "varchar",
          "Precision": 2147483647,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": True
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "class2",
          "Label": "class2",
          "Type": "varchar",
          "Precision": 2147483647,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": True
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "content",
          "Label": "content",
          "Type": "varchar",
          "Precision": 2147483647,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": True
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "epoch_time",
          "Label": "epoch_time",
          "Type": "bigint",
          "Precision": 19,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": False
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "event_date",
          "Label": "event_date",
          "Type": "varchar",
          "Precision": 2147483647,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": True
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "event_epoch_time",
          "Label": "event_epoch_time",
          "Type": "bigint",
          "Precision": 19,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": False
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "feature",
          "Label": "feature",
          "Type": "varchar",
          "Precision": 2147483647,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": True
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "headline",
          "Label": "headline",
          "Type": "varchar",
          "Precision": 2147483647,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": True
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "impacted_locations",
          "Label": "impacted_locations",
          "Type": "varchar",
          "Precision": 2147483647,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": True
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "probability1",
          "Label": "probability1",
          "Type": "double",
          "Precision": 17,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": False
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "probability2",
          "Label": "probability2",
          "Type": "double",
          "Precision": 17,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": False
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "publication_date",
          "Label": "publication_date",
          "Type": "varchar",
          "Precision": 2147483647,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": True
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "severity",
          "Label": "severity",
          "Type": "integer",
          "Precision": 10,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": False
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "summary",
          "Label": "summary",
          "Type": "varchar",
          "Precision": 2147483647,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": True
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "tags",
          "Label": "tags",
          "Type": "varchar",
          "Precision": 2147483647,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": True
        },
        {
          "CatalogName": "hive",
          "SchemaName": "",
          "TableName": "",
          "Name": "headline_original",
          "Label": "headline_original",
          "Type": "varchar",
          "Precision": 2147483647,
          "Scale": 0,
          "Nullable": "UNKNOWN",
          "CaseSensitive": True
        }
      ]
    }
  },
  "ResponseMetadata": {
    "RequestId": "8f11eb12-ab52-4be4-859e-6668f2ea5bd8",
    "HTTPStatusCode": 200,
    "HTTPHeaders": {
      "content-type": "application/x-amz-json-1.1",
      "date": "Wed, 12 Jun 2019 11:55:04 GMT",
      "x-amzn-requestid": "8f11eb12-ab52-4be4-859e-6668f2ea5bd8",
      "content-length": "33858",
      "connection": "keep-alive"
    },
    "RetryAttempts": 0
  }
}

def fetch_tags(tag_string):
    tags = ''
    tag_json = json.loads(tag_string)
    counter = 0
    for tag in tag_json:
        word = " ".join(re.findall("[a-zA-Z]+", tag['S']))
        if len(word) > 3:
            counter += 1
            word = word.capitalize()
            tags = tags + word + ','
        if counter == 5:
            break
    return tags

def fetch_lat_long(location_string):
    location = json.loads(location_string)
    lat_long_dictt = {}
    lat_long_dictt.update({"lat": location[0]['M']['lat_long']['M']['lat']['S'], "lng": location[0]['M']['lat_long']['M']['long']['S']})
    return lat_long_dictt

def fetch_event_location(loc_string):
    print(loc_string)
    print("after loc_string")
    loc_string_json = json.loads(loc_string)
    j = loc_string_json[0]
    table_data_location = ""
    if 'NULL'  not in j['M']['city'] and 'none' not in j['M']['city']['S']:
        location = j['M']['city']['S']
    elif 'NULL' not in j['M']['state'] and 'none' not in j['M']['state']['S']:
        location = j['M']['state']['S']
    else:
        location = j['M']['Country']['S']
    if location != 'none':
        table_data_location += location+","
    return table_data_location



records = cd['ResultSet']['Rows'][1]['Data']

print("before locatiion")
location = fetch_event_location(records[12]['VarCharValue'])
print("after location")
if len(location) > 0:
    location = location.rstrip(",")

    dictt = {}
    dictt.update({"event_Id": records[0]['VarCharValue'], "event_Type": records[5]['VarCharValue'],
        "severity": records[16]['VarCharValue'], "headline": records[11]['VarCharValue'],
        "summary": records[17]['VarCharValue'],
        "event_date": records[8]['VarCharValue'],
        "keywords": fetch_tags(records[18]['VarCharValue']),
        "lat_long": fetch_lat_long(records[12]['VarCharValue']),
        "location": location
            })
print(dictt)


