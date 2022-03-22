
from datetime import datetime
import re,gc, os
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyzer = SentimentIntensityAnalyzer()
import awswrangler as wr
#from sqlalchemy import create_engine
import s3fs

def handler(event, context):    
    def tweet_sentiment(tweet_df):
        scores,sentiment=[],[]
        for i in range(len(tweet_df['text'])):
            score = analyzer.polarity_scores(tweet_df['text'][i])
            score = score['compound']
            scores.append(score)

        for i in scores:
            if i>= 0.05:
                sentiment.append('Positive')
            elif i<= (-0.05):
                sentiment.append('Negative')
            else:
                sentiment.append('Neutral')
        tweet_df['score'] = scores
        tweet_df['sentiment'] = sentiment
        return tweet_df

    def clean(tweet):
        whitespace = re.compile(r"\s+")
        alpha = re.compile('[^a-zA-Z0-9 \n\.]')
        web_address = re.compile(r"(?i)http(s):\/\/[a-z0-9.~_\-\/]+")
        tesla = re.compile(r"(?i)@Tesla(?=\b)")
        user = re.compile(r"(?i)@[a-z0-9_]+")
        regrex_pattern = re.compile(pattern = "["
            u"\U0001F600-\U0001F64F"  
            u"\U0001F300-\U0001F5FF"  
            u"\U0001F680-\U0001F6FF"  
            u"\U0001F1E0-\U0001F1FF"  
                                "]+", flags = re.UNICODE)
       
        my_regex = [whitespace,web_address,tesla,user,regrex_pattern,alpha]
        for rege in my_regex:
            tweet = rege.sub(' ', tweet)
        
        return tweet

    file_name = event['Records'][0]['s3']['object']['key'] 
    bucketName=event['Records'][0]['s3']['bucket']['name'] 
    print("FILE NAME: ", file_name)
    print("BUCKET NAME: ", bucketName)
    clean_tweets_bucket = os.environ['clean_tweet'] 
    raw_tweets = wr.s3.read_parquet(path=f's3://{bucketName}/{file_name}')
    raw_tweets['datetime'] = raw_tweets['datetime'].apply(lambda x: datetime.fromisoformat(str(x)).replace(tzinfo=None))
    
    # clean tweets with regex
    raw_tweets['text'] = raw_tweets['text'].apply(lambda tweet:clean(tweet))

    # attache sentiments to tweets
    tweets_with_sentiments = tweet_sentiment(raw_tweets)

    wr.s3.to_parquet(
    df=tweets_with_sentiments,
    path=clean_tweets_bucket,
    dataset=True,
    partition_cols=['datetime'],
    database='tweets_db',  # Athena/Glue database
    table='tweets_table1'  # Athena/Glue table
    )                             
    print("FINISHED WRITING ")
    del tweets_with_sentiments
    gc.collect()
    return
    