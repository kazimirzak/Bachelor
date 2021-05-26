from pprint import pprint
import codecs
import spacy
import csv
import time
from spacy import displacy
from pymongo import MongoClient
from collections import Counter

nlp = spacy.load('en_core_web_sm')

client = MongoClient('mongodb://localhost:27017')
db = client.Bachelor


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


def read_from_database(label, keyword):
    pipeline = [
        {
            '$project': {
                '_id': 0,
                'uniqueId': 1,
                'date': 1,
                label: 1,
                'title': 1,
                'description': 1,
                'vader': 1.,
                'year': {'$year': '$date'}
            }
        }, {
            '$match': {
                label: True,
                'year': {'$gte': 2020}

            }
        }, {
            '$group': {
                '_id': '$uniqueId',
                'title': {
                    '$first': '$title'
                },
                'description': {
                    '$first': '$description'
                },
                'num_of_covid': {
                    '$sum': 1
                }
            }
        }, {
            '$match': {
                '$or': [
                    {
                        'title': {
                            '$regex': f'.*World War II.*',
                            '$options': 'i'
                        }
                    }, {
                        'description': {
                            '$regex': f'.*World War II.*',
                            '$options': 'i'
                        }
                    }, {
                        'title': {
                            '$regex': f'.*WWII.*',
                            '$options': 'i'
                        }
                    }, {
                        'description': {
                            '$regex': f'.*WWII.*',
                            '$options': 'i'
                        }
                    }
                ]
            }
        }
    ]
    test = get_data(pipeline)
    return test


if __name__ == '__main__':
    print("Lets fuck shit up!")
    labels = ['is_covid', 'is_vaccine']
    keywords = ['amazon']
    for label_ in labels:
        for keyword_ in keywords:
            data = get_data([
                {
                    '$project': {
                        'year': {'$year': '$date'},
                        label_: 1
                    }
                }, {
                    '$match': {
                        'year': {'$gte': 2020},
                        label_: True
                    }
                }, {
                    '$group': {
                        '_id': None,
                        'count': {'$sum': 1}
                    }
                }
            ])
            for i in data:
                print(i)
            print(label_, keyword_)
            data = read_from_database(label_, keyword_)
            with codecs.open(f"{label_}_WWWWWWW.txt", 'w', encoding='utf-8') as file:
                for i in data:
                    file.write(str(i))
                    file.write("\n")
