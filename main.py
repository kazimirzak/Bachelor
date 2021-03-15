import mysql.connector
import os
import fnmatch
import re
from dateutil import parser
import gzip
import json
DATA_PATH = "E:\\release"

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="secret",
    database="bachelor"
)
cursor = db.cursor()


# Inserts articles into the database using the DATA_PATH. Structure of data should be as described here: http://sciride.org/news.html#datacontent
def insert_articles():
    insert_query = "INSERT INTO articles (outlet, date, articleId, title, description, link) VALUES (%s, %s, %s, %s, %s, %s)"
    for path, dirs, files in os.walk(DATA_PATH):
        for f in fnmatch.filter(files, '*.gz'):
            abs_path = os.path.abspath(os.path.join(path, f))
            match = re.search(r'\\([^\\]*)\\per_day\\(\d*)\.gz$', abs_path)
            outlet = match.group(1).strip()
            date = parser.parse(match.group(2))
            with gzip.open(abs_path) as zip:
                content = json.loads(zip.read())
                articles = []
                for articleId, article in content.items():
                    title = article['title'].strip()
                    description = article['description'].strip()
                    link = next(iter(article['link'])).strip()
                    if title and description:
                        articles.append((outlet, date, articleId.strip(), title, description, link))
                try:
                    cursor.executemany(insert_query, articles)
                    db.commit()
                except:
                    print(outlet, date, articleId, title, description, link, sep='\n')


if __name__ == '__main__':
    insert_articles()



