import codecs
import time
from pymongo import MongoClient
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

sns.set(rc={'figure.figsize': (11, 4)})
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


def read_from_database():
    pipeline = [
        {
            '$project': {
                '_id': 0,
                'uniqueId': 1,
                'date': 1,
                'is_vaccine': 1,
                'title': 1,
                'description': 1,
                'vader': 1
            }
        }, {
            '$match': {
                'is_vaccine': True
            }
        }, {
            '$group': {
                '_id': {
                    'date': '$date',
                    'uniqueId': '$uniqueId'
                },
                'title': {
                    '$first': '$title'
                },
                'description': {
                    '$first': '$description'
                },
                'vader': {
                    '$first': '$vader'
                }
            }
        }, {
            '$match': {
                '$or': [
                    {
                        'title': {
                            '$regex': '.*AstraZeneca.*',
                            '$options': 'i'
                        }
                    }, {
                        'description': {
                            '$regex': '.*AstraZeneca.*',
                            '$options': 'i'
                        }
                    }
                ]
            }
        }, {
            '$project': {
                'date': '$_id.date',
                'vader': 1
            }
        }, {
            '$group': {
                '_id': '$date',
                'accVader': {
                    '$sum': '$vader'
                },
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$project': {
                'date': '$_id',
                'vader': {
                    '$divide': [
                        '$accVader', '$count'
                    ]
                }
            }
        }
    ]
    test = get_data(pipeline)
    return test


if __name__ == '__main__':
    print("Lets fuck shit up!")
    data = read_from_database()
    x = []
    y = []
    for i in data:
        x.append(i['date'])
        y.append(i['vader'])
    xlabel = 'Date'
    ylabel = 'Sentiment'
    df = pd.DataFrame(data={xlabel: x, ylabel: y})
    fig, axes = plt.subplots()
    sns.lineplot(data=df, x=xlabel, y=ylabel, ax=axes)
    axes.set_title("Sentiment of AstraZeneca")
    axes.set_xlabel(xlabel)
    axes.set_ylabel(ylabel)
    plt.show()
