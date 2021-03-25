import mysql.connector
import os
import fnmatch
import re
from dateutil import parser
import gzip
import json
import time
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
from math import floor

DATA_PATH = "E:\\release"

db = mysql.connector.connect(
    user="root",
    password="",
    database="bachelor"
)
cursor = db.cursor()

vader = SentimentIntensityAnalyzer()


# Inserts articles into the database using the DATA_PATH. Structure of data should be as described here: http://sciride.org/news.html#datacontent
def insert_articles():
    insert_query = "INSERT INTO articles (uniqueId, outlet, date, title, description, link) VALUES (%s, %s, %s, %s, %s, %s)"
    for path, dirs, files in os.walk(DATA_PATH):
        for f in fnmatch.filter(files, '*.gz'):
            abs_path = os.path.abspath(os.path.join(path, f))
            match = re.search(r'\\([^\\]*)\\per_day\\(\d*)\.gz$', abs_path)
            outlet = match.group(1).strip()
            date = parser.parse(match.group(2))
            with gzip.open(abs_path) as zip:
                content = json.loads(zip.read())
                articles = []
                for article_id, article in content.items():
                    title = article['title'].strip()
                    description = article['description'].strip()
                    link = next(iter(article['link'])).strip()
                    if title and description:
                        articles.append((article_id, outlet, date, title, description, link))
                try:
                    cursor.executemany(insert_query, articles)
                    db.commit()
                except:
                    print(outlet, date, title, description, link, sep='\n')


def sentiment_analysis():
    insert_vader = "INSERT INTO vader (uniqueArticleId, neg, neu, pos, compound) VALUES (%s, %s, %s, %s, %s)"

    insert_textblob = "INSERT INTO textblob (uniqueArticleId, polarity, subjectivity)" \
                      "VALUES (%s, %s, %s)"
    limit = 5000
    print("Fetching articles")
    cursor.execute("SELECT DISTINCT(uniqueId), title, description FROM articles")
    rows = cursor.fetchall()
    print("Done!")
    num_of_rows = len(rows)
    print(f"Number of insertion in total: {num_of_rows}")
    i = 0
    vs = []
    ts = []
    for article in rows:
        uniqueArticleId = article[0]
        title = article[1]
        description = article[2]
        vs.append(vader_sentiment(uniqueArticleId, f"{title} {description}"))
        ts.append(textblob_sentiment(uniqueArticleId, f"{title} {description}"))
        if i % 5000 == 0:
            cursor.executemany(insert_vader, vs)
            db.commit()
            cursor.executemany(insert_textblob, ts)
            db.commit()
            vs = []
            ts = []
            print(f"Inserting Articles: {floor(i/num_of_rows) * 100}% Done!\r")
        i = i + 1


def vader_sentiment(uniqueArticleId, text):
    sentiment = vader.polarity_scores(text)
    # return a tuple with:
    # (articleId, neg, neu, pos, compound).
    return (uniqueArticleId, sentiment['neg'], sentiment['neu'], sentiment['pos'], sentiment['compound'])


def textblob_sentiment(uniqueArticleId, text):
    sentiment = TextBlob(text).sentiment
    # Returns a tuple with:
    # (articleId, polarity, subjectivity)
    return (uniqueArticleId, sentiment.polarity, sentiment.subjectivity)


if __name__ == '__main__':
    print("Lets fuck shit up!")
    start_time = time.time()
    #insert_articles()
    sentiment_analysis()
    print("--- %s seconds ---" % (time.time() - start_time))



