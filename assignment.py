#!/usr/bin/env python
# coding: utf-8

# # Data Acquisition and Processing Systems (DaPS) (ELEC0136)    
# ### Final Assignment
# ---
# 
# ##### Before the code: The code will take some time to run and the code for the following part is included but not runned 
# 1. The locally stored datasets: Some dataset are downloaded from kaggle reuires the API token stored locally in ~/.kaggle file. The process cannot be slove with pip install kaggle. The download method will be provided but not run to download. Addtionally, AQI is more than 600 MB after download and natural disasters is 5 MB. Therefore, the datasets are preprocessed to filter the information then stored to online dataset. Please refer to aqi_natural_disaster_download(), disasters_cleaning() and disasters_cleaning() for the acquisition and cleaning steps.
# 2. The twitter data scraping which required to run the commond lines under the root dictionary.
# 3. The sentiment analysis of the news, Twitters and financial reports since it uses the pretrained FinBERT which needs the parameters downloaded. Please refer to Financial_Statements_Sentiment_Analysis.ipynb for Sentiment Analysis.
# 

# <div class="alert alert-heading alert-info">
# 
# #### Task 1: Data Acquisition
# 
# You will first have to acquire the necessary data for conducting your study. One essential type of
# data that you will need, are the stock prices for each company from April 2017 to April 202 1 as
# described in Section 1. Since these companies are public, the data is made available online. The
# first task is for you to search and collect this data, finding the best way to access and download
# it. A good place to look is on platforms that provide free data relating to the stock market such as
# Google Finance or Yahoo! Finance.
# 
# [Optional] Providing more than one method to acquire the very same or different data, e.g. from
# a downloaded comma-separated-value file and a web API, will result in a higher score.
# 
# There are many valuable sources of information for analysing the stock market. In addition to time
# series depicting the evolution of stock prices, acquire auxiliary data that is likely to be useful for
# the forecast, such as:
# 
# - Social Media, e.g., Twitter: This can be used to uncover the public’s sentimental
# response to the stock market
# - Financial reports: This can help explain what kind of factors are likely to affect the stock
# market the most
# - News: This can be used to draw links between current affairs and the stock market
# - Climate data: Sometimes weather data is directly correlated to some companies’ stock
# prices and should therefore be taken into account in financial analysis
# - Others: anything that can justifiably support your analysis.
# 
# Remember, you are looking for historical data, not live data.
#    
#     
# </div>

# In[48]:


import pandas as pd
import datetime
import seaborn as sns
sns.set_style('whitegrid')
from os import listdir
from os.path import isfile, join
import json
import http.client


def get_historical_data(symbol):
    symbol = symbol.upper()
    url_string = 'https://query1.finance.yahoo.com/v7/finance/download/{0}'.format(symbol)
    url_string += '?period1=1491004800&period2=1619740800&interval=1d&events=history&includeAdjustedClose=true'
    stocks = pd.read_csv(url_string, header=0)     
    df = pd.DataFrame(stocks)
    df.to_csv('Data_Acquisition/AAPL.csv')
    return df


# In[49]:


def get_news():
    conn = http.client.HTTPSConnection("seeking-alpha.p.rapidapi.com")
    headers = {
        'x-rapidapi-host': "seeking-alpha.p.rapidapi.com",
        'x-rapidapi-key': "a9fb207b8dmsh83a78c55c93a78ep1c0653jsn2dbaa2273e16"
        }
    mylist=[]
    for i in range(58):
        url="/news/v2/list-by-symbol?id=aapl&until=1619740800&since=1491004800&size=40&number={0}".format(i+1)
        conn.request("GET", url, headers=headers)

        res = conn.getresponse()
        data = res.read()
        mylist.append(data.decode("utf-8"))

    # print(mylist)
    with open('Data_Acquisition/news.txt', 'a') as f:
        for item in mylist:
            f.write(item.encode("gbk", 'ignore').decode("gbk", "ignore"))
            f.write('\n')                  


# In[50]:


def news_cleaning():
    with open("Data_Acquisition/news.txt") as f:
        information = f.read()
        txt_news=information
    list_news=txt_news.split('\"publishOn\":')

    list_time=[]
    list_news_title=[]
    for i in range(len(list_news)-1):
        thestring=list_news[i+1]
        thelist=thestring.split('\"title\":')
        thetitle=thelist[-1].split('},\"relationships\":')
        thetitle=thetitle[0]
        if len(thetitle)>200:
            thetitle=thelist[-2].split('},\"relationships\":')
            thetitle=thetitle[0]
        thetitle=thetitle[1:-1]
        list_news_title.append(thetitle)

        thetime=thelist[0].split(',\"isLockedPro\"')
        thetime=thetime[0].split(':')
        thetime=thetime[0]
        thetime=thetime[1:-3]
        list_time.append(thetime)
#         print(thetime,thetitle)

    #     if thetitle=='{"data":[{"id":"3687799","type":"news","attributes":{':
    #         print(list_news[i+1])

    news_data = {'Date': list_time, 'News_Title': list_news_title}
    news_df = pd.DataFrame(data=news_data)
#     print(news_df)
    news_df.to_csv('Data_Acquisition/news.csv', index=False)


# In[51]:


def get_cleaning_cov_data():
    covid_url = 'https://covid19.who.int/WHO-COVID-19-global-data.csv'
    covid_df = pd.read_csv(covid_url)
    filtered_covid_df = covid_df.groupby('Date_reported')['New_cases'].sum().reset_index()
    filtered_covid_de_df = covid_df.groupby('Date_reported')['New_deaths'].sum().reset_index()
    filtered_covid_de_df = filtered_covid_de_df.drop(['Date_reported'], axis=1)
    filtered_covid_df = pd.concat([filtered_covid_df, filtered_covid_de_df], axis=1, join='inner')
    filtered_covid_df = filtered_covid_df.drop(filtered_covid_df[(filtered_covid_df['Date_reported'] > '2021-04-30')].index)
#     print(filtered_covid_df)
    filtered_covid_df.to_csv('Data_Acquisition/covid19_sumed.csv',index = False)


# In[52]:


def get_cleaning_carbon_data():
    co2_emssion_df = pd.read_csv('https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_trend_gl.csv', skiprows=60)
    filtered_co2_emssion_df = co2_emssion_df.loc[(co2_emssion_df['year'] > 2016)]
    list_the_date=[]
    for i in range(len(filtered_co2_emssion_df)):
        the_year=filtered_co2_emssion_df['year'].iloc[i]
        the_month=filtered_co2_emssion_df['month'].iloc[i]
        the_day=filtered_co2_emssion_df['day'].iloc[i]
        the_date = datetime.date(the_year,the_month,the_day)        
        list_the_date.append(the_date)

    filtered_co2_emssion_df.insert(0, "date", list_the_date)
    filtered_co2_emssion_df = filtered_co2_emssion_df.drop(['year','month','day'], axis=1)
#     print(filtered_co2_emssion_df)
    filtered_co2_emssion_df.to_csv('Data_Acquisition/Carbon_Emission.csv', index=False)


# In[53]:


def cleaning_sort_df_date(df):
    df['Date'] = pd.to_datetime(df['Date'])
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    df = df.sort_values(by="Date")
    df.reset_index(drop=True, inplace=True)
    return df


# In[54]:


def get_cleaning_other_data():
    CBOE_vix_df = pd.read_csv('https://query1.finance.yahoo.com/v7/finance/download/%5EVIX?period1=1491004800&period2=1619740800&interval=1d&events=history&includeAdjustedClose=true')
    CBOE_vix_df = CBOE_vix_df.rename(columns={"Close": "VIX_Close"})
    CBOE_vix_df.to_csv('Data_Acquisition/CBOE_Volatility_Index.csv', index=False)
    
    dic_oil_df={'Date':[],'Oil_Close':[]}
    dic_gold_df={'Date':[],'Gold_Close':[]}
    dic_silver_df={'Date':[],'Silver_Close':[]}
    dic_dollar_index_df={'Date':[],'Dollar_Index_Close':[]}
    dic_gbp_df={'Date':[],'GBP_Close':[]}
    dic_cny_df={'Date':[],'CNY_Close':[]}
    dic_eur_df={'Date':[],'EUR_Close':[]}
    dic_nasdaq_df={'Date':[],'NASDAQ_Close':[]}
    dic_djia_df={'Date':[],'DJIA_Close':[]}
    dic_nya_df={'Date':[],'NYA_Close':[]}
    dic_rlg_df={'Date':[],'RLG_Close':[]}
    dic_spx_df={'Date':[],'SPX_Close':[]}

    start_dates=['04/03/2017','04/02/2018','04/01/2019','04/01/2020','04/01/2021']
    end_dates=['03/30/2018','03/29/2019','03/31/2020','03/31/2021','04/30/2021']
    for i in range(len(start_dates)):
        temp_df = pd.read_csv('https://www.marketwatch.com/investing/future/cl.1/downloaddatapartial?startdate={0}%2000:00:00&enddate={1}%2000:00:00&daterange=d30&frequency=p1d&csvdownload=true&downloadpartial=false&newdates=false'.format(start_dates[i],end_dates[i]))
        dic_oil_df['Date'] += temp_df['Date'].to_list()
        dic_oil_df['Oil_Close'] += temp_df['Close'].to_list()
    oil_df = cleaning_sort_df_date(pd.DataFrame(data=dic_oil_df))

    for i in range(len(start_dates)):
        temp_df = pd.read_csv('https://www.marketwatch.com/investing/future/gc00/downloaddatapartial?startdate={0}%2000:00:00&enddate={1}%2000:00:00&daterange=d30&frequency=p1d&csvdownload=true&downloadpartial=false&newdates=false'.format(start_dates[i],end_dates[i]))
        dic_gold_df['Date'] += temp_df['Date'].to_list()
        dic_gold_df['Gold_Close'] += temp_df['Close'].to_list()
    dic_gold_df['Gold_Close'] = [float(i.replace(',','')) for i in dic_gold_df['Gold_Close']]
    gold_df = cleaning_sort_df_date(pd.DataFrame(data=dic_gold_df))

    for i in range(len(start_dates)):
        temp_df = pd.read_csv('https://www.marketwatch.com/investing/future/si00/downloaddatapartial?startdate={0}%2000:00:00&enddate={1}%2000:00:00&daterange=d30&frequency=p1d&csvdownload=true&downloadpartial=false&newdates=false'.format(start_dates[i],end_dates[i]))
        dic_silver_df['Date'] += temp_df['Date'].to_list()
        dic_silver_df['Silver_Close'] += temp_df['Close'].to_list()
    silver_df = cleaning_sort_df_date(pd.DataFrame(data=dic_silver_df))

    for i in range(len(start_dates)):
        temp_df = pd.read_csv('https://www.marketwatch.com/investing/index/dxy/downloaddatapartial?startdate={0}%2000:00:00&enddate={1}%2000:00:00&daterange=d30&frequency=p1d&csvdownload=true&downloadpartial=false&newdates=false'.format(start_dates[i],end_dates[i]))
        dic_dollar_index_df['Date'] += temp_df['Date'].to_list()
        dic_dollar_index_df['Dollar_Index_Close'] += temp_df['Close'].to_list()
    dollar_index_df = cleaning_sort_df_date(pd.DataFrame(data=dic_dollar_index_df))

    for i in range(len(start_dates)):
        temp_df = pd.read_csv('https://www.marketwatch.com/investing/currency/gbpusd/downloaddatapartial?startdate={0}%2000:00:00&enddate={1}%2000:00:00&daterange=d30&frequency=p1d&csvdownload=true&downloadpartial=false&newdates=false'.format(start_dates[i],end_dates[i]))
        dic_gbp_df['Date'] += temp_df['Date'].to_list()
        dic_gbp_df['GBP_Close'] += temp_df['Close'].to_list()
    gbp_df = cleaning_sort_df_date(pd.DataFrame(data=dic_gbp_df))

    for i in range(len(start_dates)):
        temp_df = pd.read_csv('https://www.marketwatch.com/investing/currency/cnyusd/downloaddatapartial?startdate={0}%2000:00:00&enddate={1}%2023:59:59&daterange=d30&frequency=p1d&csvdownload=true&downloadpartial=false&newdates=false'.format(start_dates[i],end_dates[i]))
        dic_cny_df['Date'] += temp_df['Date'].to_list()
        dic_cny_df['CNY_Close'] += temp_df['Close'].to_list()
    cny_df = cleaning_sort_df_date(pd.DataFrame(data=dic_cny_df))

    for i in range(len(start_dates)):
        temp_df = pd.read_csv('https://www.marketwatch.com/investing/currency/eurusd/downloaddatapartial?startdate={0}%2000:00:00&enddate={1}%2000:00:00&daterange=d30&frequency=p1d&csvdownload=true&downloadpartial=false&newdates=false'.format(start_dates[i],end_dates[i]))
        dic_eur_df['Date'] += temp_df['Date'].to_list()
        dic_eur_df['EUR_Close'] += temp_df['Close'].to_list()
    eur_df = cleaning_sort_df_date(pd.DataFrame(data=dic_eur_df))

    for i in range(len(start_dates)):
        temp_df = pd.read_csv('https://www.marketwatch.com/investing/index/comp/downloaddatapartial?startdate={0}%2000:00:00&enddate={1}%2000:00:00&daterange=d30&frequency=p1d&csvdownload=true&downloadpartial=false&newdates=false'.format(start_dates[i],end_dates[i]))
        dic_nasdaq_df['Date'] += temp_df['Date'].to_list()
        dic_nasdaq_df['NASDAQ_Close'] += temp_df['Close'].to_list()
    dic_nasdaq_df['NASDAQ_Close'] = [float(i.replace(',','')) for i in dic_nasdaq_df['NASDAQ_Close']]
    nasdaq_df = cleaning_sort_df_date(pd.DataFrame(data=dic_nasdaq_df))

    for i in range(len(start_dates)):
        temp_df = pd.read_csv('https://www.marketwatch.com/investing/index/djia/downloaddatapartial?startdate={0}%2000:00:00&enddate={1}%2000:00:00&daterange=d30&frequency=p1d&csvdownload=true&downloadpartial=false&newdates=false'.format(start_dates[i],end_dates[i]))
        dic_djia_df['Date'] += temp_df['Date'].to_list()
        dic_djia_df['DJIA_Close'] += temp_df['Close'].to_list()
    dic_djia_df['DJIA_Close'] = [float(i.replace(',','')) for i in dic_djia_df['DJIA_Close']]
    djia_df = cleaning_sort_df_date(pd.DataFrame(data=dic_djia_df))

    for i in range(len(start_dates)):
        temp_df = pd.read_csv('https://www.marketwatch.com/investing/index/nya/downloaddatapartial?startdate={0}%2000:00:00&enddate={1}%2023:59:59&daterange=d30&frequency=p1d&csvdownload=true&downloadpartial=false&newdates=false'.format(start_dates[i],end_dates[i]))
        dic_nya_df['Date'] += temp_df['Date'].to_list()
        dic_nya_df['NYA_Close'] += temp_df['Close'].to_list()
    dic_nya_df['NYA_Close'] = [float(i.replace(',','')) for i in dic_nya_df['NYA_Close']]
    nya_df = cleaning_sort_df_date(pd.DataFrame(data=dic_nya_df))

    for i in range(len(start_dates)):
        temp_df = pd.read_csv('https://www.marketwatch.com/investing/index/rlg/downloaddatapartial?startdate={0}%2000:00:00&enddate={1}%2000:00:00&daterange=d30&frequency=p1d&csvdownload=true&downloadpartial=false&newdates=false'.format(start_dates[i],end_dates[i]))
        dic_rlg_df['Date'] += temp_df['Date'].to_list()
        dic_rlg_df['RLG_Close'] += temp_df['Close'].to_list()
    dic_rlg_df['RLG_Close'] = [float(i.replace(',','')) for i in dic_rlg_df['RLG_Close']]
    rlg_df = cleaning_sort_df_date(pd.DataFrame(data=dic_rlg_df))

    for i in range(len(start_dates)):
        temp_df = pd.read_csv('https://www.marketwatch.com/investing/index/spx/downloaddatapartial?startdate={0}%2000:00:00&enddate={1}%2000:00:00&daterange=d30&frequency=p1d&csvdownload=true&downloadpartial=false&newdates=false'.format(start_dates[i],end_dates[i]))
        dic_spx_df['Date'] += temp_df['Date'].to_list()
        dic_spx_df['SPX_Close'] += temp_df['Close'].to_list()
    dic_spx_df['SPX_Close'] = [float(i.replace(',','')) for i in dic_spx_df['SPX_Close']]
    spx_df = cleaning_sort_df_date(pd.DataFrame(data=dic_spx_df))
    
    dfs = [oil_df,gold_df,silver_df,dollar_index_df,gbp_df,cny_df,eur_df,nasdaq_df,djia_df,nya_df,rlg_df, spx_df]
    names = ['oil','gold','silver','dollar_index','gbp','cny','eur','nasdaq','djia','nya','rlg', 'spx']
    for i, df in enumerate(dfs):
        file_path = 'Data_Acquisition/' +names[i] + '.csv'
        df.to_csv(file_path, index=False)


# In[55]:


def twitter_cleaning():
    mypath = 'Data_Acquisition/tweet'
    list_date_twitter = []
    list_content_twitter = []
    for f in listdir(mypath):
        with open(mypath+'/'+f, 'rb') as file:
            data = json.load(file)
            data = str(data)
    #         thedata = data['created_at']
    #         thecontent = data['full_text']
            thedata = data.split('\'created_at\'')
            thedata = thedata[1]
            thedata = thedata.split('\'id\'')
            thedata = thedata[0]
            thedata = thedata[3:-3]
            thecontent = data.split('\'full_text\'')
            thecontent = thecontent[1]
            thecontent = thecontent.split('\'truncated\'')
            thecontent = thecontent[0]
            thecontent = thecontent[3:-3]
            print(thedata, thecontent)
        list_date_twitter.append(thedata)
        list_content_twitter.append(thecontent)
        
    Month_dic = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06','Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}
    list_fdate_twitter = []

    for i in range(len(list_date_twitter)):
        thestr=list_date_twitter[i]
        list_str = thestr.split(' ')
        themonth = Month_dic[list_str[1]]
        theday = str(list_str[2])
        theyear = str(list_str[-1])
        thedate = theyear+ '-' +themonth+ '-' +theday
        list_fdate_twitter.append(thedate)
#         print(thedate)

    csv_dic = {'Date':list_fdate_twitter, 'Content':list_content_twitter}
    twitter_df = pd.DataFrame(data=csv_dic)
    twitter_df = twitter_df.sort_values(by=['Date'])
#     print(twitter_df)
    twitter_df.to_csv('Data_Acquisition/Twitter.csv', index=False)


# In[56]:


def acquire():
    aapl_data = get_historical_data('AAPL')
    aapl_data.to_csv('AAPL.csv',index = False)
    
    get_news()
    news_cleaning()
    
    get_cleaning_cov_data()
    get_cleaning_carbon_data()
    get_cleaning_other_data()


# ##### The oringal data for AQI and natural disaster are not placed in the acquire part due to the memory size. The cleaned data is used instead. Please refer to the functions below about the cleaning of the data.
# #####  Twitter data is stored under the Twitter file. The acquition of Twitter information is explained as below:
# Download the files from https://github.com/jonbakerfish/TweetScraper  
# Then run the following commonds in the root folder of this project:  
#    - scrapy crawl TweetScraper -a query="aapl min_faves:1000 since:2017-04-01 until:2021-04-30 lang:en"  
#    - scrapy crawl TweetScraper -a query="@apple min_faves:2000 since:2017-04-01 until:2021-04-30 lang:en"  
#    - scrapy crawl TweetScraper -a query="#apple min_faves:5000 since:2017-04-01 until:2021-04-30 lang:en"  

# In[57]:


def aqi_natural_disaster_download():
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files('threnjen/40-years-of-air-quality-index-from-the-epa-daily/')
    api.dataset_download_files('brsdincer/all-natural-disasters-19002021-eosdis/')
    import zipfile
    with zipfile.ZipFile('40-years-of-air-quality-index-from-the-epa-daily.zip', 'r') as zip_ref:
        zip_ref.extractall('')
    with zipfile.ZipFile('all-natural-disasters-19002021-eosdis.zip', 'r') as zip_ref:
        zip_ref.extractall('')
    aqi_df = pd.read_csv('aqi_daily_1980_to_2021.csv')
    disas_df = pd.read_csv('DISASTERS/1970-2021_DISASTERS.xlsx - emdat data.csv')
    return aqi_df, disas_df


# In[58]:


def disasters_cleaning():
    _, Disasters_df = aqi_natural_disaster_download()
    filtered_Dis_df = Disasters_df.drop(Disasters_df[(Disasters_df.Year < 2017)].index)
    filtered_Dis_df = filtered_Dis_df.filter(items=['Year','Start Month','Start Day','End Year','End Month','End Day','Disaster Group', 'Disaster Type','Total Affected','Total Deaths'])
    filtered_Dis_df=filtered_Dis_df.dropna(subset=['Start Month','Start Day','End Year','End Month','End Day'])

    list_start_date=[]
    list_end_date=[]
    for i in range(len(filtered_Dis_df)):
        start_date=str(filtered_Dis_df['Year'].iloc[i])+'-'
        if len(str(filtered_Dis_df['Start Month'].iloc[i])[:-2])==1:
            start_date+='0'+str(filtered_Dis_df['Start Month'].iloc[i])[:-2]+'-'
        else:
            start_date+=str(filtered_Dis_df['Start Month'].iloc[i])[:-2]+'-'
        if len(str(filtered_Dis_df['Start Day'].iloc[i])[:-2])==1:
            start_date+='0'+str(filtered_Dis_df['Start Day'].iloc[i])[:-2]
        else:
            start_date+=str(filtered_Dis_df['Start Day'].iloc[i])[:-2]
        list_start_date.append(start_date)
        end_date=str(filtered_Dis_df['End Year'].iloc[i])+'-'
        if len(str(filtered_Dis_df['End Month'].iloc[i])[:-2])==1:
            end_date+='0'+str(filtered_Dis_df['End Month'].iloc[i])[:-2]+'-'
        else:
            end_date+=str(filtered_Dis_df['End Month'].iloc[i])[:-2]+'-'
        if len(str(filtered_Dis_df['End Day'].iloc[i])[:-2])==1:
            end_date+='0'+str(filtered_Dis_df['End Day'].iloc[i])[:-2]
        else:
            end_date+=str(filtered_Dis_df['End Day'].iloc[i])[:-2]
        list_end_date.append(end_date)
    filtered_Dis_df.insert(1, "End_Date", list_end_date)
    filtered_Dis_df.insert(1, "Start_Date", list_start_date)
    filtered_Dis_df = filtered_Dis_df.drop(['Start Month','Start Day','End Year','End Month','End Day'], axis=1)

    filtered_Dis_df= filtered_Dis_df.sort_values(by=['Start_Date'])
    filtered_Dis_df = pd.read_csv('Data_Acquisition/Natural_Disaster .csv')

    startdate = datetime.date(2017, 4, 1)
    duedate = datetime.date(2021, 4, 30)

    for i in range(len(filtered_Dis_df)):
        thedate = datetime.datetime.strptime(filtered_Dis_df['Start_Date'][i], "%Y-%m-%d").date()
        if thedate < startdate or thedate > duedate:
            filtered_Dis_df = filtered_Dis_df.drop([i])
    filtered_Dis_df.to_csv('Data_Acquisition/Natural_Disaster.csv', index=False)


# In[59]:


def aqi_cleaning():
    aqi_df,_ = aqi_natural_disaster_download()
    filtered_aqi_df = aqi_df.copy()
    filtered_aqi_df = filtered_aqi_df.drop(['Latitude','Longitude','State Name','Defining Parameter'], axis=1)
    filtered_aqi_df = filtered_aqi_df.dropna()
    filtered_aqi_df = filtered_aqi_df[filtered_aqi_df['Category']!='Good']
    filtered_aqi_df  = filtered_aqi_df[filtered_aqi_df['Category']!='Moderate']
    filtered_aqi_df = filtered_aqi_df.sort_values(by=['Date'])
    filtered_aqi_df= filtered_aqi_df.loc[(filtered_aqi_df['Date'] > '2017-03-31')]
    filtered_aqi_df.to_csv('Bad_AQI.csv', index=False)
    filtered_aqi_df = pd.read_csv('Data_Acquisition/Bad_AQI.csv')
    filtered_aqi_df = filtered_aqi_df.groupby('Date')['AQI'].sum().reset_index()
    filtered_aqi_df.to_csv('Data_Acquisition/Date_Bad_AQI.csv', index=False)


# <div class="alert alert-heading alert-info">
#     
# ## Task 2: Data Storage
# 
# Once you have found a way to acquire the relevant data, you need to decide on how to store it.
# You should choose a format that allows an efficient read access to allow training a parametric
# model. Also, the data corpus should be such that it can be easily inspected. Data can be stored
# locally, on your computer.
#     
# </div>

# In[60]:


import pandas as pd
import pymongo
# pprint library is used to make the output look more pretty
from pymongo.errors import ConnectionFailure
import csv
import os
import json
import pandas as pd
import sys, getopt, pprint


# In[61]:


def csv_2_json(file_name):
    csv_path = 'Data_Acquisition/' + file_name + '.csv'
    json_path = 'Data_Storage/' + file_name + '.json'
    pd_csvfile = pd.read_csv(csv_path)
    pd_csvfile.to_json(json_path)                               # saving to json file
    jdf = open(json_path).read()                        # loading the json file 
    data = json.loads(jdf)  
    return data


# In[62]:


def read_txt(file_name):
    file_path = 'Data_Preprocessing/'+file_name + '.txt'
    f = open(file_path)  # open a file
    text = f.read()                     
    data = {file_name : text }
    return data


# In[63]:


def store_stock_indexes_reports():
    client = pymongo.MongoClient(
        'mongodb+srv://darren_zhao:Hu8FZR8edfh2b0eP@cluster0.x5znh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
    names = ['oil', 'gold', 'silver', 'dollar_index', 'gbp', 'cny', 'eur', 'nasdaq', 'djia', 'nya', 'rlg', 'spx']
    for thename in names:
        thename_capital = thename.upper()
        db = client[thename_capital]
        db.data.drop()
        data = csv_2_json(thename)
        db.data.insert_one(data)
        pprint.pprint(db.data.find_one())

    AAPL_reports = ['AAPL_report_2016', 'AAPL_report_2017', 'AAPL_report_2018', 'AAPL_report_2019', 'AAPL_report_2020',
                    'AAPL_report_2021']
    AAPL_db = client.AAPL
    for report in AAPL_reports:
        AAPL_db[report].drop()
        collection = AAPL_db[report]
        data = read_txt(report)
        collection.insert_one(data)
        pprint.pprint(collection.find_one())
    client.close()


# In[64]:


def store():
    # Implement me, and remove the exception below.
    # Make sure you return what you need.
    client = pymongo.MongoClient('mongodb+srv://darren_zhao:Hu8FZR8edfh2b0eP@cluster0.x5znh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
    for db in client.list_databases():
        print(db)
    
    AAPL_db = client.AAPL
    AAPL_db.data.drop()
    data = csv_2_json('AAPL')
    AAPL_db.data.insert_one(data)
    # db.list_collection_names()
#     pprint.pprint(AAPL_db.data.find_one())
    
    news_db = client.NEWS
    news_db.data.drop()
    data = csv_2_json('news')
    news_db.data.insert_one(data)
#     pprint.pprint(news_db.data.find_one())
    
    natural_d_db = client.NATURAL_DISASTER
    natural_d_db.data.drop()
    data = csv_2_json('Natural_Disaster')
    natural_d_db.data.insert_one(data)
#     pprint.pprint(natural_d_db.data.find_one())
    
    aqi_db = client.AQI
    aqi_db.data.drop()
    data = csv_2_json('Date_Bad_AQI')
    aqi_db.data.insert_one(data)
#     pprint.pprint(aqi_db.data.find_one())
    
    carbon_db = client.CARBON_EMISSION
    carbon_db.data.drop()
    data = csv_2_json('Carbon_Emission')
    carbon_db.data.insert_one(data)
#     pprint.pprint(carbon_db.data.find_one())
    
    cboe_db = client.CBOE
    cboe_db.data.drop()
    data = csv_2_json('CBOE_Volatility_Index')
    cboe_db.data.insert_one(data)
#     pprint.pprint(cboe_db.data.find_one())

    covid_db = client.COVID
    covid_db.data.drop()
    data = csv_2_json('covid19_sumed')
    covid_db.data.insert_one(data)
    
    twitter_db = client.TWITTER
    twitter_db.data.drop()
    data = csv_2_json('Twitter')
    twitter_db.data.insert_one(data)
#     pprint.pprint(twitter_db.data.find_one())
    client.close()


# <div class="alert alert-heading alert-warning">
# 
# [Optional] Create a simple API to allow Al retrieving the compound of data you collected. It is enough to provide a single access point to retrieve all the data, and not implement query mechanism. The API must be accessible from the web. If you engage in this task data must be stored online.  
#     
# </div>

# In[65]:


def retrieve(data):
    client = pymongo.MongoClient('mongodb+srv://darren_zhao:Hu8FZR8edfh2b0eP@cluster0.x5znh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
    the_db = client[data]
    cursor = the_db.data.find_one()
    # print(cursor['Date'])
    dic_output = {}

    for thekey in cursor.keys():
        if thekey == '_id':
            continue
    #     print(cursor[thekey].values())
        dic_output[thekey] = list(cursor[thekey].values())

    df =  pd.DataFrame(data= dic_output)
    client.close()
    return df


# In[66]:


df = retrieve('AAPL_DATA')
df


# <div class="alert alert-heading alert-info">
# 
# ## Task 3: Data Preprocessing
# 
# Now that you have the data stored, you can start preprocessing it. Think about what features to
# keep, which ones to transform, combine or discard. Make sure your data is clean and consistent
# (e.g., are there many outliers? any missing values?). You are expected to:
# 
# 1. Clean the data from missing values and outliers, if any.
# 2. Provide useful visualisation of the data. Plots should be saved on disk, and not printed on
# the juptyer notebook.
# 3. Transform your data (e.g., using normalization, dimensionality reduction, etc.) to improve
# the forecasting performance.
# 
# </div>

# In[67]:


from utils import expand_contractions, remove_accented_chars, scrub_words, review_to_words
import re
import scipy
from scipy import stats
import datetime
import numpy as np

def financial_report_data_cleaning():
    client = pymongo.MongoClient('mongodb+srv://darren_zhao:Hu8FZR8edfh2b0eP@cluster0.x5znh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
    AAPL_reports = ['AAPL_report_2016','AAPL_report_2017','AAPL_report_2018','AAPL_report_2019','AAPL_report_2020','AAPL_report_2021']
    AAPL_db = client.AAPL
    for report in AAPL_reports:
        cursor = AAPL_db[report].find_one()
        dic_output = {}

        for thekey in cursor.keys():
            if thekey == '_id':
                continue
        #     print(cursor[thekey].values())
            dic_output[thekey] = cursor[thekey]
        information = list(dic_output.values())
        information = information[0]
        
        #using contractions dictionary to make corrections 
        information = expand_contractions(re.sub('’', "'", information))

        #stripping the words using space 
        information = information.strip().lower()

        #removing accented characters 
        information = remove_accented_chars(information) 

        #re-placing " " " with space 
        information = information.replace('"', '')

        # Removing url's from the text
        url_reg  = r'[a-z]*[:.]+\S+'
        information = re.sub(url_reg, '', information)

        information = re.sub(r"\b[a-zA-Z]\b", "", information)

        #removing scrub_words
        information = scrub_words(information)

        #replace spaaces more than one with single space 
        information = re.sub("\s+", ' ', information) 

#         information = review_to_words(information)
        word_count = len(information.split(' '))
        print(word_count)
        print(information)
        file_name = 'Cleaned_' + report
        AAPL_db[file_name].drop()
        collection = AAPL_db[file_name]
        data = {file_name : information }
        collection.insert_one(data)
#         pprint.pprint(collection.find_one())

    client.close()


# In[68]:


def Tiwtter_Data_Cleaning():
    twitter_df = retrieve('TWITTER')
    Cleaned_str = []
    client = pymongo.MongoClient('mongodb+srv://darren_zhao:Hu8FZR8edfh2b0eP@cluster0.x5znh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
    for i in range(len(twitter_df)):
        thestr = twitter_df['Content'].iloc[i]
        thestr = expand_contractions(re.sub('’', "'", thestr))

        #stripping the words using space 
        thestr = thestr.strip().lower()

        #removing accented characters 
        thestr = remove_accented_chars(thestr) 

        #re-placing " " " with space 
        thestr = thestr.replace('"', '')

        # Removing url's from the text
        url_reg  = r'[a-z]*[:.]+\S+'
        thestr = re.sub(url_reg, '', thestr)

        thestr = re.sub(r"\b[a-zA-Z]\b", "", thestr)

        #removing scrub_words
        thestr = scrub_words(thestr)

        #replace spaaces more than one with single space 
        thestr = re.sub("\s+", ' ', thestr)
        twitter_df['Content'].iloc[i] = thestr
        
    cleaned_twitter_db = client['CLEANED_TWITTER']
    cleaned_twitter_db.data.drop()
    twitter_df.to_json('Data_Preprocessing/Cleaned_Twitter.json')                               # saving to json file
    jdf = open('Data_Preprocessing/Cleaned_Twitter.json').read()                        # loading the json file 
    data = json.loads(jdf)
    cleaned_twitter_db.data.insert_one(data)
#     pprint.pprint(cleaned_twitter_db.data.find_one())


# In[69]:


def week_days_update(update_name, merged_df):
    for i in range(len(merged_df)):    
        if np.isnan(merged_df['Open'].iloc[i]):          
            the_score = merged_df[update_name].iloc[i]
            print(merged_df['Date'].iloc[i])
            the_Date = datetime.datetime.strptime(merged_df['Date'].iloc[i], '%Y-%m-%d')
            thedate = the_Date + datetime.timedelta(days=1)
            thedate = str(thedate).split(' ')[0]
            while True:                  
                the_index = merged_df[merged_df['Date'] == thedate].index.values
                if len(the_index)==0:
                    thedate = datetime.datetime.strptime(thedate, '%Y-%m-%d') + datetime.timedelta(days=1)
                    thedate = str(thedate).split(' ')[0]
                    print(thedate)
                    continue
                the_index = the_index[0]
                if np.isnan(merged_df['Open'].iloc[the_index]) ==False:
                    merged_df[update_name].iloc[the_index] += the_score
                    break
                else:
                    thedate = datetime.datetime.strptime(thedate, '%Y-%m-%d') + datetime.timedelta(days=1)
                    thedate = str(thedate).split(' ')[0]
                    print(thedate)
    
        


# In[70]:


def data_contactnaing_outlier_removing():
    client = pymongo.MongoClient('mongodb+srv://darren_zhao:Hu8FZR8edfh2b0eP@cluster0.x5znh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
    AAPL_df = retrieve('AAPL')
    news_df = retrieve('NEWS_ANALYSIS')
    twitter_df = retrieve('TWITTER_ANALYSIS')
    report_df = retrieve('REPORT_ANALYSIS')
    aqi_df = retrieve('AQI')
    carbon_df = retrieve('CARBON_EMISSION')
    cboe_df = retrieve('CBOE')
    disaster_df = retrieve('NATURAL_DISASTER')
    covid_df = retrieve('COVID')
    
    disaster_df = disaster_df.dropna(subset=['Total Affected','Total Deaths'])
    disaster_df  = disaster_df.loc[(disaster_df['Total Deaths'] > 100)]
    disaster_df  = disaster_df.loc[(disaster_df['Total Affected'] > 5000)]
    list_output_date=[]
    list_output_influence = []
    for i in range(len(disaster_df)):
        theStart_Date = disaster_df['Start_Date'].iloc[i]
        theEnd_Date = disaster_df['End_Date'].iloc[i]
        theStart_Date = datetime.datetime.strptime(theStart_Date, '%Y-%m-%d')
        theEnd_Date = datetime.datetime.strptime(theEnd_Date, '%Y-%m-%d')
        difference =  (theEnd_Date - theStart_Date).days
        for ii in range(difference):
            thedate = theStart_Date+ datetime.timedelta(days=ii)
            thedate = str(thedate).split(' ')[0]
            list_output_date.append(thedate)
            list_output_influence.append(disaster_df['Total Affected'].iloc[i]/difference)
    dic_output = {'Date':list_output_date,'Disaster_Affected':list_output_influence}
    disaster_df  = pd.DataFrame(data=dic_output)
    disaster_df  = disaster_df.groupby('Date')['Disaster_Affected'].sum().reset_index()

    merged_df = pd.merge(AAPL_df,disaster_df,how='left',on='Date') 
    merged_df['Disaster_Affected'] = merged_df['Disaster_Affected'].fillna(0)

    news_df = retrieve('NEWS_ANALYSIS')
    news_df = news_df.groupby('Date').sum()
    news_df['News_Score'] = news_df['positive_score'] - news_df['negative_score']
    news_df = news_df.reset_index()

    merged_df = pd.merge(merged_df,news_df[['Date','News_Score']],how='outer',on='Date') 
    merged_df['News_Score'] = merged_df['News_Score'].fillna(0)
    week_days_update('News_Score',merged_df)
    merged_df=merged_df.dropna(subset=['Close'])
    merged_df

    twitter_df = retrieve('TWITTER_ANALYSIS')
    twitter_df = twitter_df.groupby('Date').sum()
    twitter_df['Twitter_Score'] = twitter_df['positive_score'] - twitter_df['negative_score']
    twitter_df = twitter_df.reset_index()
    twitter_df['Date'] = pd.to_datetime(twitter_df['Date'])
    twitter_df['Date'] = twitter_df['Date'].dt.strftime('%Y-%m-%d')

    merged_df = pd.merge(merged_df,twitter_df[['Date','Twitter_Score']],how='outer',on='Date') 
    merged_df['Twitter_Score'] = merged_df['Twitter_Score'].fillna(0)
    week_days_update('Twitter_Score',merged_df)
    merged_df=merged_df.dropna(subset=['Close'])
    
    report_df  = report_df.groupby('Year').sum().reset_index()
    report_df['Report_Score'] = report_df['positive_score'] - report_df['negative_score']

    temp_df=merged_df.copy()
    set_date='09-26'
    list_temp=[]
    for i in range(len(temp_df)):
        the_date=merged_df['Date'].iloc[i].split('-')
        the_year=int(the_date[0])
        the_index = the_year-2017
        month_date = the_date[1] +'-'+ the_date[2]
        if month_date>set_date:
            the_index += 1
        list_temp.append(report_df['Report_Score'].iloc[the_index])
    
    temp_df['Report_Score']=list_temp
    merged_df = temp_df
    
    temp_df = pd.merge(temp_df,aqi_df,how='left',on='Date') 
    carbon_df = carbon_df.rename(columns={"date": "Date"})
    carbon_df = carbon_df.rename(columns={"trend": "Carbon_Smoothed"})
    temp_df = pd.merge(temp_df,carbon_df[['Date','Carbon_Smoothed']],how='left',on='Date') 
    
    covid_df = covid_df.rename(columns={"New_deaths": "Covid_Index"})
    covid_df = covid_df.rename(columns={"Date_reported": "Date"})
    temp_df = pd.merge(temp_df,covid_df[['Date','Covid_Index']],how='left',on='Date') 
    temp_df["Covid_Index"] = temp_df["Covid_Index"].fillna(0)
    
    temp_df = pd.merge(temp_df,cboe_df[['Date','VIX_Close']],how='left',on='Date') 
    names = ['oil','gold','silver','dollar_index','gbp','cny','eur','nasdaq','djia','nya','rlg', 'spx']
    for thename in names:
        thename_capital = thename.upper()
        df = retrieve(thename_capital)
        temp_df = pd.merge(temp_df,df,how='left',on='Date') 
        
    temp_df['VIX_Close'] = temp_df['VIX_Close'].fillna(0)
    temp_df['AQI'] = temp_df['AQI'].fillna(0)
    temp_df.to_csv('Before_data.csv')
    
    list_temp = scipy.ndimage.gaussian_filter(list_temp, 45)
    temp_df['Report_Score']=list_temp
    
    the_title = 'Open'
    threshold = 3
    for col in temp_df.columns:
        the_title = str(col)
        if the_title == 'Date':
            continue
#         print(temp_df[the_title].isnull().any())
#         print(the_title)
        outlier_column = np.array(temp_df[the_title])
        outlier_date = np.array(temp_df['Date'])
#         print(outlier_column)
        z = np.abs(stats.zscore(outlier_column))
        # sns.set_style('whitegrid')

        outlier_loc = np.where(z > threshold)
        # find the outlier value given its index
        outlier_by_Z_Score = outlier_column[outlier_loc]
        
        print('the data classified as outlier by z score:\r', outlier_by_Z_Score)
        print('the date of the outlier is:\r', outlier_date[outlier_loc])

        dropped_outlier_column = np.delete(outlier_column, outlier_loc)
        # do the same for year value
        capped_outlier_column = np.copy(outlier_column)
        print('Before cap the outlier, its value:\r\n', capped_outlier_column[outlier_loc])
        # cap the outliers
        new_max = np.max(dropped_outlier_column)
        new_min = np.min(dropped_outlier_column)
        outlier_index = outlier_loc[0].tolist()
        for item in outlier_index:
            #     print(item)
            #     print(new_max)
            if capped_outlier_column[item] > new_max:
                capped_outlier_column[item] = new_max
            elif capped_outlier_column[item] < new_min:
                capped_outlier_column[item] = new_min
            else:
                print('error')
        temp_df[the_title] = list(capped_outlier_column)
        print('After cap the outlier, its value:\r\n', capped_outlier_column[outlier_loc])
        print('------------------------------------------------------------------------------------')
        
    for col in temp_df.columns:
        if col == 'Date':
            continue
        temp_df[col] =(temp_df[col]-temp_df[col].min())/(temp_df[col].max()-temp_df[col].min())
        
    aapl_data_db = client.AAPL_DATA
    aapl_data_db.data.drop()
    temp_df.to_json('Data_Storage/AAPL_Data.json')                               # saving to json file
    jdf = open('Data_Storage/AAPL_Data.json').read()                                 # saving to json file                     # loading the json file 
    data = json.loads(jdf)  
    aapl_data_db.data.insert_one(data)
#     pprint.pprint(aapl_data_db.data.find_one()) 
    client.close()


# In[71]:


def process():
    financial_report_data_cleaning()
    Tiwtter_Data_Cleaning()
    data_contactnaing_outlier_removing()


# ##### The sentiment analysis of the data works as below:
# Download the model from https://github.com/ProsusAI/finBERT and the model parameters from https://huggingface.co/ProsusAI/finbert/tree/main  Move the model parameters to the dictionay specified in Finbert  
# Please refer to Data_Preprocessing/Financial_Statements_Sentiment_Analysis.ipynb for the sentiment analysis.  
# The output data are store at 'NEWS_ANALYSIS', 'TWITTER_ANALYSIS' AND 'REPORT_ANALYSIS' in MongoDB.

# <div class="alert alert-heading alert-info">
#     
# ## Task 4: Data Exploration
# 
# After ensuring that the data is well preprocessed, it is time to start exploring the data to carry out
# hypotheses and intuition about possible patterns that might be inferred. Depending on the data,
# different EDA (exploratory data analysis) techniques can be applied, and a large amount of
# information can be extracted.
# For example, you could do the following analysis:
# 
#     
# - Time series data is normally a combination of several components:
#   - Trend represents the overall tendency of the data to increase or decrease over time.
#   - Seasonality is related to the presence of recurrent patterns that appear after regular
# intervals (like seasons).
#   - Random noise is often hard to explain and represents all those changes in the data
# that seem unexpected. Sometimes sudden changes are related to fixed or predictable
# events (i.e., public holidays).
# - Features correlation provides additional insight into the data structure. Scatter plots and
# boxplots are useful tools to spot relevant information.
# - Explain unusual behaviour.
# - Explore the correlation between stock price data and other external data that you can
# collect (as listed in Sec 2.1)
# - Use hypothesis testing to better understand the composition of your dataset and its
# representativeness.
# 
#     
# At the end of this step, provide key insights on the data. This data exploration procedure should
# inform the subsequent data analysis/inference procedure, allowing one to establish a predictive
# relationship between variables.
# 
# </div>

# In[72]:


import matplotlib.pyplot as plt
from matplotlib.pylab import datestr2num
from calendar import  mdays
import datetime as dt
# import datetime
import seaborn as sns
import statsmodels.api as sm

from scipy.stats import chi2_contingency
from scipy.stats import chi2


# In[73]:


def time_series_analysis_by_year(AAPL_df):
    temp_df = AAPL_df[['Date','Close','Disaster_Affected','News_Score','Twitter_Score','Report_Score','AQI','Carbon_Smoothed','Covid_Index','VIX_Close',
                       'Oil_Close','Gold_Close','Silver_Close','Dollar_Index_Close','GBP_Close','CNY_Close','EUR_Close','NASDAQ_Close',
                       'DJIA_Close','NYA_Close','RLG_Close','SPX_Close']]
    temp_df['Date'] = pd.to_datetime(temp_df['Date'])
    g = temp_df.groupby(pd.Grouper(key='Date', freq='M'))
    dfs = [group for _,group in g]
    deivided = len(g)//12
    output_group = []
    for i in range(deivided):
        for ii in range(12):
            if ii == 0:
                the_group=dfs[ii+12*i]
            else:
                the_group = the_group.append(dfs[ii+12*i])
        output_group.append(the_group)
    output_month_group = {}
    for i in range(deivided):
        for ii in range(12):
            if i==0 and ii==0:
                the_group = dfs[ii+12*i].sum()
                for thekey in the_group.keys():
    #                 print(thekey)
    #                 output_month_group[thekey] =the_group[thekey].tolist()
                    the_list=[]
                    the_list.append(the_group[thekey])
                    output_month_group[thekey] = the_list
            else:
                the_value = dfs[ii+12*i].sum()
                for thekey in the_group.keys():
    #                 print(output_month_group[thekey],the_value[thekey])
                    output_month_group[thekey].append(the_value[thekey])
    #                 the_group[thekey] = np.append(the_group[thekey],the_value[thekey])
    
    list_thedates=[]
    for i in range(deivided):
        for ii in range(12):
            theyear = 2017 +i
            theyear = str(theyear)
            themonth=(ii+4)%12
            themonth=str(themonth)
            thedatetime = theyear+'-'+themonth
    #         thedatetime = datetime.datetime.strptime((theyear+themonth), "%Y%m").date()
            list_thedates.append(thedatetime)
    print(list_thedates)
    df = pd.DataFrame(data=output_month_group)
    df.index=list_thedates
    
    for item in output_group:
    #     item['Date'] = pd.to_datetime(item['Date'])
        # Set the column 'Date' as index (skip if already done)
    #     item = item.set_index('Date')
        seasonal_cycle = item.rolling(window=90, center=True).mean().groupby(item['Date']).mean()
        seasonal_cycle.plot()
        plt.clf() 
#         plt.show()
    
    the_names=['Close','Disaster_Affected','News_Score','Twitter_Score','Report_Score','AQI','Carbon_Smoothed','Covid_Index','VIX_Close',
                       'Oil_Close','Gold_Close','Silver_Close','Dollar_Index_Close','GBP_Close','CNY_Close','EUR_Close','NASDAQ_Close',
                       'DJIA_Close','NYA_Close','RLG_Close','SPX_Close']
    for thekey in the_names:   
        for i,item in enumerate(output_group):
            the_title = i+2017
            the_title = "Financial Year " + str(the_title) + ': ' + str(thekey)
            item.sort_index(inplace= True)
            res = sm.tsa.seasonal_decompose(np.asarray(item[thekey]), period=21, model='additive')
            res.plot()
            plt.suptitle(the_title)
            plt.subplots_adjust(top=0.8)
            plt.clf() 
#             plt.show()


# In[74]:


def time_series_analysis(AAPL_df):
    the_names=['Close','Disaster_Affected','News_Score','Twitter_Score','Report_Score','AQI','Carbon_Smoothed','Covid_Index','VIX_Close',
                       'Oil_Close','Gold_Close','Silver_Close','Dollar_Index_Close','GBP_Close','CNY_Close','EUR_Close','NASDAQ_Close',
                       'DJIA_Close','NYA_Close','RLG_Close','SPX_Close']
    for thekey in the_names:   
        the_title = "Time Series Analysis" +  ': ' + str(thekey)
        res = sm.tsa.seasonal_decompose(np.asarray(AAPL_df[thekey]),period=21, model='additive')
        res.plot()
        plt.suptitle(the_title)
        plt.subplots_adjust(top=0.8)
        plt.clf()
#         plt.show()


# In[75]:


def Feature_Correlation(AAPL_df):
    
    the_names=['Close','Disaster_Affected','News_Score','Twitter_Score','Report_Score','AQI','Carbon_Smoothed','Covid_Index','VIX_Close',
                   'Oil_Close','Gold_Close','Silver_Close','Dollar_Index_Close','GBP_Close','CNY_Close','EUR_Close','NASDAQ_Close',
                   'DJIA_Close','NYA_Close','RLG_Close','SPX_Close']
    # x = AAPL_df['Close']
    # y = AAPL_df['Disaster_Affected']
    cor_AAPL_df = AAPL_df[the_names]
    # Covariance = np.cov(x, y)
    Covariance = cor_AAPL_df.corr()
    Covariance = Covariance.round(decimals=2, out=None)
    print(Covariance)
    fig, ax = plt.subplots(figsize=(12,10)) 
    sns.heatmap(Covariance, annot=True, fmt='g')
    plt.title("Correlation matrix of AAPL Stock Price")
    plt.savefig('Correlation_matrix_of_AAPL_Stock_Price.png')
    plt.clf()
#     plt.show()


# In[76]:


def append_diff_df(df,thekeys):
    for key in thekeys:
        the_diffs = df[key].diff()
        str_key = key.split('_')
        str_key = str_key[0] + '_Change'        
        df = df.assign(temp_change=np.where(the_diffs > 0, 1, np.where(the_diffs < 0, 0, 0)))
        df = df.rename(columns={"temp_change": str_key})
        df[str_key] = df[str_key].astype(int) 
    return df


# In[77]:


def positive_negative_information(AAPL_df):
    mean_dic={}
    min_dic={}
    for item in AAPL_df.columns:
        if item == 'Date':
            continue
        mean_dic[item] = AAPL_df[item].mean()
        min_dic[item] = AAPL_df[item].min()
        
    diffs = AAPL_df['Close'].diff()
    temp_df = AAPL_df.copy()
    temp_df = temp_df.assign(Change=np.where(diffs > 0, 1, np.where(diffs < 0, 0, 0)))
    temp_df['Change'] = temp_df['Change'].astype(int) 
    increase_no = len(temp_df[temp_df['Change'] == 1])
    decline_no = len(temp_df[temp_df['Change'] == 0])

    need_diff = ['VIX_Close','Oil_Close','Gold_Close','Silver_Close','Dollar_Index_Close','GBP_Close','CNY_Close','EUR_Close','NASDAQ_Close',
                   'DJIA_Close','NYA_Close','RLG_Close','SPX_Close']
    temp_df = append_diff_df(temp_df,need_diff)
    print(temp_df)
   
    positve_disaster_no = len(temp_df[(temp_df['Disaster_Affected']> min_dic['Disaster_Affected'] ) & (temp_df['Change'] == 1)])
    negative_disaster_no = len(temp_df[(temp_df['Disaster_Affected']> min_dic['Disaster_Affected'] ) & (temp_df['Change'] == 0)])
    positve_not_disaster_no = len(temp_df[(temp_df['Disaster_Affected']<= min_dic['Disaster_Affected'] ) & (temp_df['Change'] == 1)])
    negative_not_disaster_no = len(temp_df[(temp_df['Disaster_Affected']<= min_dic['Disaster_Affected'] ) & (temp_df['Change'] == 0)])
    
    positve_good_carbon_no = len(temp_df[(temp_df['Carbon_Smoothed']> mean_dic['Carbon_Smoothed'] ) & (temp_df['Change'] == 1)])
    negative_good_carbon_no = len(temp_df[(temp_df['Carbon_Smoothed']> mean_dic['Carbon_Smoothed'] ) & (temp_df['Change'] == 0)])
    positve_bad_carbon_no = len(temp_df[(temp_df['Carbon_Smoothed']<= mean_dic['Carbon_Smoothed'] ) & (temp_df['Change'] == 1)])
    negative_bad_carbon_no = len(temp_df[(temp_df['Carbon_Smoothed']<= mean_dic['Carbon_Smoothed'] ) & (temp_df['Change'] == 0)])
    
    positve_good_air_no = len(temp_df[(temp_df['AQI']> mean_dic['AQI'] ) & (temp_df['Change'] == 1)])
    negative_good_air_no = len(temp_df[(temp_df['AQI']> mean_dic['AQI'] ) & (temp_df['Change'] == 0)])
    positve_bad_air_no = len(temp_df[(temp_df['AQI']<= mean_dic['AQI'] ) & (temp_df['Change'] == 1)])
    negative_bad_air_no = len(temp_df[(temp_df['AQI']<= mean_dic['AQI'] ) & (temp_df['Change'] == 0)])
    
    positve_good_covid_no = len(temp_df[(temp_df['Covid_Index']> mean_dic['Covid_Index'] ) & (temp_df['Change'] == 1)])
    negative_good_covid_no = len(temp_df[(temp_df['Covid_Index']> mean_dic['Covid_Index'] ) & (temp_df['Change'] == 0)])
    positve_bad_covid_no = len(temp_df[(temp_df['Covid_Index']<= mean_dic['Covid_Index'] ) & (temp_df['Change'] == 1)])
    negative_bad_covid_no = len(temp_df[(temp_df['Covid_Index']<= mean_dic['Covid_Index'] ) & (temp_df['Change'] == 0)])
    
    positve_good_news = len(temp_df[(temp_df['News_Score']> mean_dic['News_Score'] ) & (temp_df['Change'] == 1)])
    negative_good_news = len(temp_df[(temp_df['News_Score']> mean_dic['News_Score'] ) & (temp_df['Change'] == 0)])
    positve_bad_news = len(temp_df[(temp_df['News_Score']<= mean_dic['News_Score'] ) & (temp_df['Change'] == 1)])
    negative_bad_news = len(temp_df[(temp_df['News_Score']<= mean_dic['News_Score'] ) & (temp_df['Change'] == 0)])
    
    positve_good_twitters = len(temp_df[(temp_df['Twitter_Score']> mean_dic['Twitter_Score'] ) & (temp_df['Change'] == 1)])
    negative_good_twitters = len(temp_df[(temp_df['Twitter_Score']> mean_dic['Twitter_Score'] ) & (temp_df['Change'] == 0)])
    positve_bad_twitters = len(temp_df[(temp_df['Twitter_Score']<= mean_dic['Twitter_Score'] ) & (temp_df['Change'] == 1)])
    negative_bad_twitters = len(temp_df[(temp_df['Twitter_Score']<= mean_dic['Twitter_Score'] ) & (temp_df['Change'] == 0)])
    
    positve_good_reports = len(temp_df[(temp_df['Report_Score']> mean_dic['Report_Score'] ) & (temp_df['Change'] == 1)])
    negative_good_reports = len(temp_df[(temp_df['Report_Score']> mean_dic['Report_Score'] ) & (temp_df['Change'] == 0)])
    positve_bad_reports = len(temp_df[(temp_df['Report_Score']<= mean_dic['Report_Score'] ) & (temp_df['Change'] == 1)])
    negative_bad_reports = len(temp_df[(temp_df['Report_Score']<= mean_dic['Report_Score'] ) & (temp_df['Change'] == 0)])
    
    res_values = []
    for item in need_diff:
        str_key = item.split('_')
        str_key = str_key[0] + '_Change'  
        positve_increase_temp_Close = len(temp_df[(temp_df[str_key]==1 ) & (temp_df['Change'] == 1)])
        negative_increase_temp_Close = len(temp_df[(temp_df[str_key]==1 ) & (temp_df['Change'] == 0)])
        positve_decline_temp_Close = len(temp_df[(temp_df[str_key]==0 ) & (temp_df['Change'] == 1)])
        negative_decline_temp_Close = len(temp_df[(temp_df[str_key]==0 ) & (temp_df['Change'] == 0)])
        res_values.append([positve_increase_temp_Close, negative_increase_temp_Close, positve_decline_temp_Close,
                          negative_decline_temp_Close])
    
       
    list_disaster_no =[positve_disaster_no, negative_disaster_no, positve_disaster_no + negative_disaster_no]
    list_not_disaster_no =[positve_not_disaster_no, negative_not_disaster_no, positve_not_disaster_no + negative_not_disaster_no]
    
    list_good_carbon_no =[positve_good_carbon_no, negative_good_carbon_no, positve_good_carbon_no + negative_good_carbon_no]
    list_bad_carbon_no =[positve_bad_carbon_no, negative_bad_carbon_no, positve_bad_carbon_no + negative_bad_carbon_no]
    
    list_good_air_no =[positve_good_air_no, negative_good_air_no, positve_good_air_no + negative_good_air_no]
    list_bad_air_no =[positve_bad_air_no, negative_bad_air_no, positve_bad_air_no + negative_bad_air_no]
    
    list_good_covid_no =[positve_good_covid_no, negative_good_covid_no, positve_good_covid_no + negative_good_covid_no]
    list_bad_covid_no =[positve_bad_covid_no, negative_bad_covid_no, positve_bad_covid_no + negative_bad_covid_no]
    
    list_good_news =[positve_good_news, negative_good_news, positve_good_news + negative_good_news]
    list_bad_news =[positve_bad_news, negative_bad_news, positve_bad_news + negative_bad_news]
    
    list_good_twitters =[positve_good_twitters, negative_good_twitters, positve_good_twitters + negative_good_twitters]
    list_bad_twitters =[positve_bad_twitters, negative_bad_twitters, positve_bad_twitters + negative_bad_twitters]
    
    list_good_reports =[positve_good_reports, negative_good_reports, positve_good_reports + negative_good_reports]
    list_bad_reports =[positve_bad_reports, negative_bad_reports, positve_bad_reports + negative_bad_reports]
    
    res_dic={}
    for i, item in enumerate(need_diff):
        str_key = item.split('_')
        increase_str_key = 'Increase_'+str_key[0] + '_days'
        decline_str_key = 'Increase_'+str_key[0] + '_days'
        list_increase_VIX_Close =[res_values[i][0], res_values[i][1], res_values[i][0]+ res_values[i][1]]
        list_decline_VIX_Close =[res_values[i][2], res_values[i][3], res_values[i][2]+ res_values[i][3]]
        res_dic[increase_str_key] = list_increase_VIX_Close
        res_dic[decline_str_key] = list_decline_VIX_Close
    
    list_total = [increase_no, decline_no, increase_no + decline_no]
    
#     d = {'Disaster_days': list_disaster_no, 'Good_Carbon_days': list_good_carbon_no, 'Good_Air_days': list_good_air_no,
#          'Good_News_days': list_good_news, 'Good_Twitters_days': list_good_twitters,'Good_Reports_days': list_good_reports,
#          'Increase_VIX_days': list_increase_VIX_Close,'Total': list_total }
    d = {'Disaster_days': list_disaster_no, 'Good_Carbon_days': list_good_carbon_no, 'Bad_Carbon_days': list_bad_carbon_no,
         'Good_Air_days': list_good_air_no, 'Bad_Air_days': list_bad_air_no,'Good_Covid_days': list_good_covid_no, 
         'Bad_Covid_days': list_bad_covid_no, 'Good_News_days': list_good_news,'Bad_News_days': list_bad_news, 
         'Good_Twitters_days': list_good_twitters, 'Bad_Twitters_days': list_bad_twitters,
         'Good_Reports_days': list_good_reports, 'Bad_Reports_days': list_bad_reports,'Increase_VIX_days': list_increase_VIX_Close,
         'Decline_VIX_days': list_decline_VIX_Close}
    d.update(res_dic)
    d['Total']= list_total
    
    index = ['Increase_Days','Decline_days', 'Total']
    Table_with_Total = pd.DataFrame(data=d,index=index)
    
    return Table_with_Total
#     print(positve_disaster_no,negative_disaster_no,positve_not_disaster_no,negative_not_disaster_no  )


# In[78]:


def test_hypothesis(AAPL_df):
    test_df = positive_negative_information(AAPL_df)
    stat, p, dof, expected = chi2_contingency(test_df)
    print("statistic",stat)
    print("p-value",p)
    print("degres of fredom: ",dof)
    print("table of expected frequencies\n",expected)

    prob = 0.90
    critical = chi2.ppf(prob, dof)
    if abs(stat) >= critical:
        print('Dependent (reject H0)')
    else:
        print('Independent (fail to reject H0)')


# In[79]:


def explore():
    AAPL_df = retrieve('AAPL_DATA')
    time_series_analysis(AAPL_df)
    time_series_analysis_by_year(AAPL_df)
    Feature_Correlation(AAPL_df)
    test_hypothesis(AAPL_df)


# <div class="alert alert-heading alert-info">
# 
# ## Task 5: Inference
# 
# Train a model to predict the closing stock price on each day for the data you have already
# collected, stored, preprocessed and explored from previous steps. The data must be spanning
# from April 2017 to April 202 1.
# You should develop two separate models:
# 
# 
# 1. A model for predicting the closing stock price on each day for a 1-month time window (until
#     end of May 202 1 ), using only time series of stock prices.
# 2. A model for predicting the closing stock price on each day for a 1-month time window (until
#     end of May 202 1 ), using the time series of stock prices and the auxiliary data you collected.
# Which model is performing better? How do you measure performance and why? How could you
# further improve the performance? Are the models capable of predicting the closing stock prices
# far into the future?
# 
# [IMPORTANT NOTE] For these tasks, you are not expected to compare model architectures, but
# examine and analyse the differences when training the same model with multiple data attributes
# and information from sources. Therefore, you should decide a single model suitable for time series
# data to solve the tasks described above. Please see the lecture slides for tips on model selection
# and feel free to experiment before selecting one.
# 
# The following would help you evaluate your approach and highlight potential weaknesses in your
# process:
# 
# 1. Evaluate the performance of your model using different metrics, e.g. mean squared error,
#     mean absolute error or R-squared.
# 2. Use ARIMA and Facebook Prophet to explore the uncertainty on your model’s predicted
#     values by employing confidence bands.
# 3. Result visualization: create joint plots showing marginal distributions to understand the
#     correlation between actual and predicted values.
# 4. Finding the mean, median and skewness of the residual distribution might provide
#     additional insight into the predictive capability of the model.
# </div>

# In[80]:


import numpy as np
import pandas as pd
import os
import pymongo

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torch.utils.data import Dataset
from torch.utils.data import DataLoader

# pip install matplotlib
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
# get_ipython().run_line_magic('matplotlib', 'inline')
import seaborn as sns
train_on_gpu = torch.cuda.is_available()
from sklearn.decomposition import PCA
from sklearn import preprocessing
if not train_on_gpu:
    print('CUDA is not available.')
else:
    print('CUDA is available.')
    
from statsmodels.tsa.stattools import acf
import math
import pmdarima as pm


# In[81]:


class MyDataLoader(Dataset):
    def __init__(self, x_list, y_list):

        self.x_list = x_list
        self.y_list = y_list
        
    def __len__(self):
        return len(self.x_list)
    

    def __getitem__(self, idx):
        x_data = self.x_list[idx]
        y_data = self.y_list[idx]
        sample = {'Data': x_data, 'Labels': y_data}

        return sample
    


# In[82]:


def split_train_test_data(df, lookback,num_feature=1):
    df_list = df.values.tolist()
    res_list = df['Close'].tolist()
    res_list = np.array(res_list)
    print(len(res_list))
    res_list = np.expand_dims(res_list, axis=1)
    data = []
    res_data = []
    scaler = preprocessing.MinMaxScaler()

    # create all possible sequences of length seq_len
    if num_feature == 1:  
        for index in range(len(df_list) - lookback -20): 
            the_list = res_list[index: index + lookback]
            data.append(the_list)
            res_data.append(res_list[index + lookback:index + lookback+21])
    else:  
        pca_fit = PCA(num_feature-1)
        df_list = pca_fit.fit_transform(df_list)
        df_list=scaler.fit_transform(df_list)
        for index in range(len(df_list) - lookback -20): 
            the_list = df_list[index: index + lookback]
            data.append(the_list)
            res_data.append(res_list[index + lookback:index + lookback+21])
        
    
    data = np.array(data)
    res_data = np.array(res_data)
    
    test_set_size = int(np.round(0.1*data.shape[0]))
    train_set_size = data.shape[0] - (test_set_size)*2
    
    x_train = data[:train_set_size,:,:]
    y_train = res_data[:train_set_size]
    y_train = np.squeeze(y_train)
    
    x_val = data[train_set_size:train_set_size+test_set_size,:,:]
    y_val = res_data[train_set_size:train_set_size+test_set_size]
    y_val = np.squeeze(y_val)
    
    x_test = data[train_set_size+test_set_size:,:,:]
    y_test = res_data[train_set_size+test_set_size:]
    y_test = np.squeeze(y_test)
    
    return [x_train, y_train,x_val,y_val, x_test, y_test]


# In[83]:


class AttentionalLSTM(nn.Module):
    """LSTM with Attention"""
    def __init__(self, input_size, qkv, hidden_size, num_layers, output_size, bidirectional=False):
        super(AttentionalLSTM, self).__init__()

        self.input_size = input_size
        self.qkv = qkv
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.output_size = output_size

        self.query = nn.Linear(input_size, qkv)
        self.key = nn.Linear(input_size, qkv)
        self.value = nn.Linear(input_size, qkv)

        self.attn = nn.Linear(qkv, input_size)
        self.scale = math.sqrt(qkv)

        self.lstm = nn.LSTM(input_size=input_size,
                            hidden_size=hidden_size,
                            num_layers=num_layers,
                            batch_first=True,
                            bidirectional=bidirectional)

        if bidirectional:
            self.fc = nn.Linear(hidden_size * 2, output_size)
        else:
            self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x):

        Q, K, V = self.query(x), self.key(x), self.value(x)

        dot_product = torch.matmul(Q, K.permute(0, 2, 1)) / self.scale
        scores = torch.softmax(dot_product, dim=-1)
        scaled_x = torch.matmul(scores, V) + x

        out = self.attn(scaled_x) + x
        out, _ = self.lstm(out)
        out = out[:, -1, :]
        out = self.fc(out)

        return out


# In[84]:


def train(model, train_loader, val_loader, optimizer, criterion):
    n_epochs = 150
    last_epochs=0
    train_loss_res=[]
    valid_loss_res=[]
    valid_loss_min = np.Inf # track change in validation loss

    for epoch in range(n_epochs):
        train_loss = 0.0
        valid_loss = 0.0
        model.train()
        for i_batch, sample_batched in enumerate(train_loader):
            data_batch,labels_batch = sample_batched['Data'], sample_batched['Labels']
            data=data_batch.float()
            target = labels_batch.float()
            if train_on_gpu:
                data, target = data.cuda(), target.cuda()

            optimizer.zero_grad()
            output = model(data)
    #         print(output.size(),target.size())
            loss = criterion(output, target)
            loss.backward()
            optimizer.step()
            train_loss += loss.item()*data.size(0)


        model.eval()
        for i_batch, sample_batched in enumerate(val_loader):
            data_batch,labels_batch = sample_batched['Data'], sample_batched['Labels']
            data = data_batch.float()
            target = labels_batch.float()
            if train_on_gpu:
                data, target = data.cuda(), target.cuda()
            output = model(data)

            loss = criterion(output, target)
            valid_loss += loss.item()*data.size(0)

    #     print(len(train_loader.sampler))
        train_loss = train_loss/len(train_loader.sampler)
        valid_loss = valid_loss/len(val_loader.sampler)

        print('Epoch: {} \tTraining Loss: {:.6f} \tValidation Loss: {:.6f}'.format(
            epoch, train_loss, valid_loss))
        train_loss_res.append(train_loss)
        valid_loss_res.append(valid_loss)

        if valid_loss <= valid_loss_min:
            last_epochs=epoch
            print('Validation loss decreased ({:.6f} --> {:.6f}).  Saving model ...'.format(
            valid_loss_min,
            valid_loss))
            torch.save(model.state_dict(), 'model_cifar.pt')
            valid_loss_min = valid_loss
        if epoch-last_epochs>50:
            break
    return model


# In[85]:


def combine_values(the_list,last=True):
    res=[]
    for i in range(len(the_list)):
        if last:
            if i==len(the_list)-1:
                for ii in range(len(the_list[i])):
                    res.append(the_list[i][ii])
            else:
                res.append(the_list[i][0])
        else:
            res.append(the_list[i][0])
    res=np.array(res)
    return res


# In[86]:


def forecast_accuracy(forecast, actual):
    print(forecast.shape)
    mape = np.mean(np.abs(forecast - actual)/np.abs(actual))  # MAPE
    me = np.mean(forecast - actual)             # ME
    mae = np.mean(np.abs(forecast - actual))    # MAE
    mpe = np.mean((forecast - actual)/actual)   # MPE
    rmse = np.mean((forecast - actual)**2)**.5  # RMSE
    corr = np.corrcoef(forecast, actual)[0,1]   # corr
    mins = np.amin(np.hstack([forecast[:,None], 
                              actual[:,None]]), axis=1)
    maxs = np.amax(np.hstack([forecast[:,None], 
                              actual[:,None]]), axis=1)
    minmax = 1 - np.mean(mins/maxs)             # minmax
#     acf1 = acf(fc-test)[1]                      # ACF1
    return({'mape':mape, 'me':me, 'mae': mae, 
            'mpe': mpe, 'rmse':rmse,
#             'acf1':acf1, 
            'corr':corr, 'minmax':minmax})


# In[87]:


def evaluate(model, x_train, y_train, x_val, y_val, x_test, y_test, AAPL_df):
    actual_values = np.concatenate((y_train.detach().cpu().numpy(),  y_val.detach().cpu().numpy()), axis=0)
    actual_values = np.concatenate((actual_values,y_test.detach().cpu().numpy()), axis=0)
    print(actual_values.shape)
    actual = combine_values(actual_values)
    print(actual.shape)

    # actual =  np.concatenate(actual)
    if train_on_gpu:
        predict = model(x_train.cuda()).detach().cpu().numpy()
        val_predict = model(x_val.cuda()).detach().cpu().numpy()
        test_predict = model(x_test.cuda()).detach().cpu().numpy()
    else:
        predict = model(x_train).detach().numpy()
        val_predict = model(x_val).detach().numpy()
        test_predict = model(x_test).detach().numpy()
        
    train_num = len(predict)
    train_sum = 0 
    for i in range(train_num):
        train_sum += np.abs(actual_values[i]-predict[i])
    print('Train Loss:',train_sum.sum()/train_num)

    val_num = len(val_predict)
    val_sum = 0 
    for i in range(val_num):
        val_sum += np.abs(actual_values[train_num+i]-val_predict[i])
    print('Val Loss:',val_sum.sum()/val_num)

    test_num = len(test_predict)
    test_sum = 0 
    for i in range(test_num):
        test_sum += np.abs(actual_values[val_num+train_num+i]-test_predict[i])
    print('Test Loss:',test_sum.sum()/test_num)

    predict=combine_values(predict,False)
    print(predict.shape)
    val_predict=combine_values(val_predict,False)
    print(val_predict.shape)
    print(test_predict.shape)
    test_predict=combine_values(test_predict,True)
    print(test_predict.shape)

    val_predict = np.append(val_predict,test_predict[0])
    print(len(val_predict))
    os.environ["KMP_DUPLICATE_LIB_OK"]  =  "TRUE"

    data_date = AAPL_df['Date'].tolist()
    data_date = np.array(data_date[20:])
    data_len = len(data_date)
    fig = figure(figsize=(25, 5), dpi=300)
    fig.patch.set_facecolor((1.0, 1.0, 1.0))
    plt.plot(data_date, actual, label="Actual Prices", color='b')
    plt.plot(data_date[:len(predict)], predict, label="Predicted Training Prices", color='r')
    plt.plot(data_date[len(predict):len(predict)+len(val_predict)], val_predict, label="Predicted Validation Prices", color='g')
    plt.plot(data_date[len(predict)+len(val_predict)-1:], test_predict, label="Predicted Testing Prices", color='k')

    plt.title("Predicted Close Prices and Actual Close Prices without External Data")
    xticks = [data_date[i] if ((i%45==0 and (data_len-i) > 45) or i==data_len-1) else None for i in range(data_len)] # make x ticks nice
    x = np.arange(0,len(xticks))
    plt.xticks(x, xticks, rotation='vertical')
    plt.grid(b=None, which='major', axis='y', linestyle='--')
    plt.legend()
    plt.savefig('External_Data_Pred.png')
    plt.clf()
#     plt.show()
    
    predict = model(x_train.cuda()).detach().cpu().numpy()
    val_predict = model(x_val.cuda()).detach().cpu().numpy()
    test_predict = model(x_test.cuda()).detach().cpu().numpy()

    print(forecast_accuracy(actual_values[:train_num],predict))
    print(forecast_accuracy(actual_values[train_num:val_num+train_num],val_predict))
    print(forecast_accuracy(actual_values[val_num+train_num:],test_predict))


# In[88]:


def denormalise(Ori_AAPL_df,thevalue):
    themax=Ori_AAPL_df['Close'].max()
    themin=Ori_AAPL_df['Close'].min()
    therange = themax - themin
    thevalue = thevalue*therange+themin
#     print(thevalue)
    return thevalue


# In[89]:


def normalise(Ori_AAPL_df,thevalue):
    themax=Ori_AAPL_df['Close'].max()
    themin=Ori_AAPL_df['Close'].min()
    therange = themax - themin
    thevalue = (thevalue- themin)/therange
#     print(thevalue)
    return thevalue


# In[90]:


def arima_eval(AAPL_df):
    pred_res = AAPL_df['Close']
    # model = pm.auto_arima(pred_res, start_p=1, start_q=1,
    # #                       test='adf',       # use adftest to find optimal 'd'
    #                       max_p=10, max_q=10, # maximum p and q
    #                       m=12,              # frequency of series
    #                       d=None,           # let model determine 'd'
    #                       seasonal=False,   # No Seasonality
    #                       start_P=0, 
    #                       D=1, 
    #                       trace=True,
    #                       error_action='ignore',  
    #                       suppress_warnings=True, 
    #                       stepwise=True)

    model = pm.auto_arima(pred_res, start_p=1, start_q=1,
                             test='adf',
                             max_p=5, max_q=5, m=8,
                             start_P=0, seasonal=True,
                             d=None, D=1, trace=True,
                             error_action='ignore',  
                             suppress_warnings=True, 
                             stepwise=True)

    print(model.summary())
    model.plot_diagnostics(figsize=(7,5))
    plt.clf()
#     plt.show()
    n_periods = 21
    fc, confint = model.predict(n_periods=n_periods, return_conf_int=True)
    

    index_of_fc = np.arange(1026, 1026+n_periods)

    fc_series = pd.Series(fc, index=index_of_fc)
    lower_series = pd.Series(confint[:, 0], index=index_of_fc)
    upper_series = pd.Series(confint[:, 1], index=index_of_fc)
    may_df = pd.read_csv('https://query1.finance.yahoo.com/v7/finance/download/AAPL?period1=1491004800&period2=1622419200&interval=1d&events=history&includeAdjustedClose=true', header=0)
    may_price = may_df['Close'][-21:]
    may_price = np.array(may_price)
    
    Ori_AAPL_df = retrieve('AAPL')
    act_res = normalise(Ori_AAPL_df,may_df['Close'].tolist())
    arima_data_date = may_df['Date'].tolist()
    arima_data_date = np.array(arima_data_date)
    arima_data_len = len(arima_data_date)
    # Plot
    fig = figure(figsize=(25, 5), dpi=300)
    fig.patch.set_facecolor((1.0, 1.0, 1.0))

    plt.plot(arima_data_date,act_res)
    plt.plot(arima_data_date[-21:],fc_series, color='r')
    plt.fill_between(lower_series.index, 
                     lower_series, 
                     upper_series, 
                     color='k', alpha=.15)

    plt.title("21 Days Forecast: ARIMA from 2020-05-03 to 2021-05-28")
    xticks = [arima_data_date[i] if ((i%45==0 and (arima_data_len-i) > 45) or i==arima_data_len-1) else None for i in range(arima_data_len)] # make x ticks nice
    x = np.arange(0,len(xticks))
    plt.xticks(x, xticks, rotation='vertical')
    plt.grid(b=None, which='major', axis='y', linestyle='--')
    plt.savefig('ARIMA_eval.png')
    plt.clf()
#     plt.show()


# In[91]:


def data_inference():
    AAPL_df = retrieve('AAPL_DATA')
#     set_df = AAPL_df.drop(columns=['Date'])
    # the_names=['Open','High','Low','Close','Adj Close','Volume','Disaster_Affected','News_Score','Twitter_Score','Report_Score',
    #            'AQI','Carbon_Smoothed','Covid_Index','VIX_Close','Oil_Close','Gold_Close','Silver_Close','Dollar_Index_Close',
    #            'GBP_Close','CNY_Close','EUR_Close','NASDAQ_Close']

    the_names=['Open','High','Low','Close','Adj Close','News_Score','Twitter_Score',
               'Carbon_Smoothed','Covid_Index','Gold_Close','Dollar_Index_Close','NASDAQ_Close']
    set_df = AAPL_df[the_names]
    lookback = 20 # choose sequence length
    x_train, y_train,x_val,y_val, x_test, y_test = split_train_test_data(set_df, lookback,num_feature=1)# change the num_feature value for using more features

    # x_train = np.moveaxis(x_train, 1, -1)
    # x_val = np.moveaxis(x_val, 1, -1)
    # x_test = np.moveaxis(x_test, 1, -1)
    # print(x_test[-1])
    # print(y_test[-1])
    print('x_train.shape = ',x_train.shape)
    print('y_train.shape = ',y_train.shape)
    print('x_test.shape = ',x_test.shape)
    print('y_test.shape = ',y_test.shape)
    print('x_val.shape = ',x_val.shape)
    print('y_val.shape = ',y_val.shape)
    x_train = torch.from_numpy(x_train).type(torch.Tensor)
    x_test = torch.from_numpy(x_test).type(torch.Tensor)
    x_val = torch.from_numpy(x_val).type(torch.Tensor)
    y_train = torch.from_numpy(y_train).type(torch.Tensor)
    y_test = torch.from_numpy(y_test).type(torch.Tensor)
    y_val = torch.from_numpy(y_val).type(torch.Tensor)

    dataset_train = MyDataLoader(x_train, y_train)
    dataset_test = MyDataLoader(x_test, y_test)
    dataset_val = MyDataLoader(x_val, y_val)

    batch_size = 256
    train_loader = DataLoader(dataset=dataset_train, batch_size=batch_size, num_workers=0, pin_memory=True)
    val_loader = DataLoader(dataset=dataset_val, batch_size=batch_size, shuffle=True, num_workers=0)
    test_loader = DataLoader(dataset=dataset_test, batch_size=batch_size, shuffle=True, num_workers=0)
    
    input_dim = len(x_train[0][0])
    hidden_dim = 64
    num_layers = 1
    output_dim = 21
    print(input_dim)
    model = AttentionalLSTM(input_dim, input_dim ,hidden_dim, num_layers,output_dim)
    # model = LSTM(input_dim, hidden_dim, num_layers, output_dim)
    if train_on_gpu:
        model.cuda()
    criterion = torch.nn.L1Loss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    
    model = train(model, train_loader, val_loader, optimizer, criterion)
#     model.load_state_dict(torch.load('1_googd_model_without.pt'))
    
    evaluate(model, x_train, y_train, x_val, y_val, x_test, y_test, AAPL_df)
    arima_eval(AAPL_df)


# <div class="alert alert-heading alert-danger">
# 
# ## Autorun
# 
# </div>

# In[92]:


def main():
    acquire()
    store()
    process()
    explore()
    data_inference()


# In[93]:


main()


# In[ ]:




