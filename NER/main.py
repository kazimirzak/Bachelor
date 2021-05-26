from pprint import pprint
import codecs
import spacy
import csv
import time
from spacy import displacy
from pymongo import MongoClient
from collections import Counter
import pandas as pd
nlp = spacy.load('en_core_web_sm')

client = MongoClient('mongodb://localhost:27017')
db = client.Bachelor

label = 'is_covid'


def get_data(pipeline):
    # Expects the pipeline to be a pipelines used to aggregate a query in mongodb.
    # The resulting rows must contain '_id', 'vader', 'tb'
    # Where '_id' = labeling of said data, 'vader' = vader sentiment of the data
    # 'tb' = textblob sentiment of the data.
    print("---   Querying database   ---")
    start_time = time.time()
    data = db.Konrad.aggregate(pipeline, allowDiskUse=True)
    print("--- Done in: %.2f seconds ---" % (time.time() - start_time))
    return data


def read_from_database():
    pipeline = [
        {
            '$match': {
                label: True
            }
        }, {
            '$group': {
                '_id': '$uniqueId',
                'title': {
                    '$first': '$title'
                },
                'description': {
                    '$first': '$description'
                }
            }
        }, {
            '$project': {
                'text': {
                    '$concat': [
                        '$title', ' ', '$description'
                    ]
                }
            }
        }
    ]
    data = get_data(pipeline)
    test = []
    for i in data:
        test.append(i['text'])
    return test


def get_countries():
    pipeline = [
        {
            '$match': {
                label: True
            }
        }, {
            '$group': {
                '_id': '$country'

            }
        }
    ]
    data = get_data(pipeline)
    test = []
    for i in data:
        test.append(i['_id'])
    return test


def get_years():
    pipeline = [
        {
            '$match': {
                label: True
            }
        }, {
            '$group': {
                '_id': {'$year': '$date'}
            }
        }
    ]
    data = get_data(pipeline)
    test = []
    for i in data:
        test.append(i['_id'])
    test.sort()
    return test


def read_database(c):
    pipeline = [
        {
            '$project': {
                '_id': 0,
                'year': {
                    '$year': '$date'
                },
                label: 1,
                'country': 1,
                'title': 1,
                'description': 1,
                'uniqueId': 1
            }
        }, {
            '$match': {
                label: True,
                'country': c
            }
        }, {
            '$group': {
                '_id': '$uniqueId',
                'title': {
                    '$first': '$title'
                },
                'description': {
                    '$first': '$description'
                }
            }
        }, {
            '$project': {
                'text': {
                    '$concat': [
                        '$title', ' ', '$description'
                    ]
                }
            }
        }
    ]
    data = get_data(pipeline)
    test = []
    for i in data:
        test.append(i['text'])
    return test


if __name__ == '__main__':
    print("Lets fuck shit up!")
    print(label)
    countries = get_countries()
    print("Countries: ", countries)
    for country in countries:
        print("Processing:", country)
        lines = read_database(country)
        print("Lines:", len(lines))
        ents = []
        for line in lines:
            result = nlp(line)
            ents.extend([(x.text, x.label_) for x in result.ents])
        entities = []
        labels = []
        counts = []
        for ((entity, label_), count) in Counter(ents).most_common():
            entities.append(entity)
            labels.append(label_)
            counts.append(count)
        pf = pd.DataFrame(data={'entity': entities, 'label': labels, 'occurrences': counts})
        pf.to_csv(f'{label}_{country}.csv')
        print("------------ DONE ------------")
