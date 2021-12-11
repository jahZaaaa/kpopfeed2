import tweepy as tw
import time
# your Twitter API key and API secret
my_api_key = "kDqFVRREGy9KTSsH62KqOv0lq"
my_api_secret = "CuVoX7KijLxYcElBZHtdziiZEmppLix8v7bNZk5LGc7vj9FZY9"

# authenticate
auth = tw.OAuthHandler(my_api_key, my_api_secret)
api = tw.API(auth, wait_on_rate_limit=False)

from google.cloud import bigquery
import os

credentials_path = '/home/natnaree_j2539/kpopfeed2/kpopfeed_key.json'
#credentials_path = 'kpopfeed_key.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

client = bigquery.Client()
table_id = 'sixth-fold-334210.kpoptest_6.kpop'

from pyspark import SparkContext

sc = SparkContext.getOrCreate()

from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .appName("Pyspark_kpop")
         .getOrCreate())
print(spark)

import pandas as pd

# intialize the dataframe
tweets_df = pd.DataFrame()

from datetime import datetime, timedelta
#-7
end_date = datetime.now()
start_date = end_date-timedelta(minutes=15)

#เวลาที่เริ่มรันลบไป15นาที
print(start_date)
print(end_date)

list_group=['BTS','BLACKPINK','GIDLE','NCT','TXT','TWICE','ITZY','treasure','straykids','redvelvet','seventeen','ateez','enhypen','wayv','theboyz','everglow','stayc','Dreamcatcher',
'loona','aespa','purplekiss','weeekly','gwsn','woo!ah!']

size_search={
'BTS':300,
'BLACKPINK':200,
'GIDLE':30,
'NCT':100,
'TXT':80,
'TWICE':80,
'ITZY':80,
'treasure':80,
'straykids':80,
'redvelvet':30,
'seventeen':80,
'ateez':80,
'enhypen':90,
'wayv':80,
'theboyz':80,
'everglow':20,
'stayc':20,
'Dreamcatcher':10,
'loona':20,
'aespa':80,
'purplekiss':20,
'weeekly':10,
'gwsn':10,
'woo!ah!':10
    
}


while True:
    tweets_df = pd.DataFrame()
    end_date = datetime.now()
    start_date = end_date-timedelta(minutes=15)
    #เวลาที่เริ่มรันลบไป15นาที
    #end_date = end_date-timedelta(hours=7)
    #start_date = start_date-timedelta(hours=7)
    nnow=datetime.now()+timedelta(hours=7)
    for g in range(0,len(list_group)):
        search_query = "#"+list_group[g]+" -filter:retweets"
        print(list_group[g])
    # print(size_search[list_group[g]])
    # get tweets from the API
        tweets = tw.Cursor(api.search_tweets,
                        q=search_query,
                        lang="en",
                        since="2021-12-9",
                        sleep_on_rate_limit=False
                        ).items(size_search[list_group[g]])
        

        # store the API responses in a list
        tweets_copy = []
        for tweet in tweets:
            tweet.created_at2=tweet.created_at.replace(tzinfo=None)
            if tweet.created_at2 < end_date and tweet.created_at2 > start_date:
                tweets_copy.append(tweet)    
            
        print("Total Tweets fetched:", len(tweets_copy))
       
        for tweet in tweets_copy:
            try:
                text = tweet.text
            except:
                text=''
                pass
            tweets_df = tweets_df.append(pd.DataFrame({
                                                    'date': tweet.created_at,
                                                    'text': text ,
                                                    'group':list_group[g],
                                                    'collect_date':nnow
                                                    }, index=[0]))
            tweets_df = tweets_df.reset_index(drop=True)
    sparkDF=spark.createDataFrame(tweets_df) 
    #sparkDF.printSchema()
    #sparkDF.show()    
    sparkDF.createOrReplaceTempView("kpop")
    all_df = spark.sql("select group,COUNT(text),collect_date from kpop group by group,collect_date ")
    #all_df.show()
    all_df_p=all_df.toPandas()
    for row in all_df_p.iterrows():
        try:
            rows_to_insert = [    { u'group':row[1][0], u'count':row[1][1],u'collect_date':str(row[1][2])}      ]
            errors = client.insert_rows_json(table_id, rows_to_insert)
            if errors == []:
                print('New rows have been added.')
            else:
                print(f'Encountered errors while inserting rows: {errors}')  
        except:
            k=0    
    time.sleep(60*15)





