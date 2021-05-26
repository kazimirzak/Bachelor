import pandas as pd
import time
from pymongo import MongoClient
from dateutil import parser


client = MongoClient('mongodb://localhost:27017')
db = client.Bachelor
DATA_PATH = "E:\\bachelor\\result.csv"
NUM_ENTRIES = 27_162_218
BATCH_SIZE = 5_000


if __name__ == '__main__':
    print("Lets fuck shit up!")
    names = ["uniqueId", "outlet", "date", "title", "description", "normalized_title", "normalized_description", "is_covid", "is_vaccine", "vader", "tb", "link"]
    listify = lambda x: x[2:-2].split("', '")
    dateify = lambda x: parser.parse(x)
    converters = {'date': dateify, 'normalized_title': listify, 'normalized_description': listify}
    iterations = NUM_ENTRIES // BATCH_SIZE
    with pd.read_csv(DATA_PATH, sep=',', names=names, converters=converters, chunksize=BATCH_SIZE) as reader:
        i = 0
        for chunk in reader:
            start_time = time.time()
            articles = list(chunk.T.to_dict().values())
            db.Konrad.insert_many(articles)
            print(f"Progress: {round(i / iterations * 100)}% - time spent {(time.time() - start_time)}s")
            i += 1
