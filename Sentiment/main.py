from pymongo import MongoClient
import time
import pandas as pd

client = MongoClient('mongodb://localhost:27017')
db = client.Bachelor

label = 'is_vaccine'


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
                label: 1,
                'title': 1,
                'description': 1,
                'vader': 1
            }
        }, {
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
                },
                'vader': {
                    '$avg': '$vader'
                }
            }
        }, {
            '$project': {
                '_id': 1,
                'title': 1,
                'description': 1,
                'vader': 1
            }
        }
    ]
    data = get_data(pipeline)
    uniqueId = []
    title = []
    description = []
    vader = []
    for i in data:
        uniqueId.append(i['_id'])
        title.append(i['title'])
        description.append(i['description'])
        vader.append(i['vader'])
    data = {"UniqueId": uniqueId, 'Title': title, 'Description': description, 'Vader': vader}
    df = pd.DataFrame(data=data)
    df.to_csv(label + '.csv')


def create_brand_mapping():
    df = pd.read_csv('is_vaccine.csv', index_col=[0])
    brands = {
        'AstraZeneca': ['AstraZeneca'],
        'Biontech': ['Biontech'],
        'Pfizer': ['Pfizer'],
        'Moderna': ['Moderna'],
        'Sputnik V': ['Sputnik V'],
        'Johnson & Johnson': ['Johnson & Johnson', 'Jannsen', 'Johnson and Johnson'],
        'EpiVacCorona': ['EpiVacCorona'],
        'CoviVac': ['CoviVac']
    }
    l = []
    for index, row in df.iterrows():
        for k, v in brands.items():
            for keyword in v:
                if keyword.lower() in row['Description'].lower() or keyword.lower() in row['Title'].lower():
                    l.append((k, row['Vader']))
    brand = []
    vader = []
    for i in l:
        brand.append(i[0])
        vader.append(i[1])
    dataframe = pd.DataFrame(data={"Brand": brand, 'Vader': vader})
    dataframe.to_csv('is_vaccine_brands.csv')


def vaccine_sentiment_by_week(filename):
    pipeline = [
        {
            '$project': {
                '_id': 0,
                'vader': 1,
                'is_vaccine': 1,
                'uniqueId': 1,
                'year': {
                    '$year': '$date'
                },
                'week': {
                    '$week': '$date'
                }
            }
        }, {
            '$match': {
                'is_vaccine': True,
                'year': {
                    '$gte': 2020
                }
            }
        }, {
            '$group': {
                '_id': {
                    'year': '$year',
                    'week': '$week',
                    'uniqueId': '$uniqueId'
                },
                'vader': {
                    '$avg': '$vader'
                }
            }
        }, {
            '$group': {
                '_id': {
                    'year': '$_id.year',
                    'week': '$_id.week'
                },
                'vader': {
                    '$avg': '$vader'
                }
            }
        }, {
            '$sort': {
                '_id.year': 1,
                '_id.week': 1,
            }
        }, {
            '$project': {
                'period': {
                    '$concat': [
                        '0-',
                        {'$toString': '$_id.week'},
                        '-',
                        {'$toString': '$_id.year'}
                    ]
                },
                'vader': '$vader'
            }
        }
    ]
    read_database(pipeline, filename)


def vaccine_sentiment_by_month(filename):
    pipeline = [
        {
            '$project': {
                '_id': 0,
                'vader': 1,
                'is_vaccine': 1,
                'uniqueId': 1,
                'year': {
                    '$year': '$date'
                },
                'month': {
                    '$month': '$date'
                }
            }
        }, {
            '$match': {
                'is_vaccine': True,
                'year': {
                    '$gte': 2020
                }
            }
        }, {
            '$group': {
                '_id': {
                    'year': '$year',
                    'month': '$month',
                    'uniqueId': '$uniqueId'
                },
                'vader': {
                    '$avg': '$vader'
                }
            }
        }, {
            '$group': {
                '_id': {
                    'year': '$_id.year',
                    'month': '$_id.month'
                },
                'vader': {
                    '$avg': '$vader'
                }
            }
        }, {
            '$sort': {
                '_id.year': 1,
                '_id.month': 1,
            }
        }, {
            '$project': {
                'period': {
                    '$concat': [
                        {'$toString': '$_id.month'},
                        '-',
                        {'$toString': '$_id.year'}
                    ]
                },
                'vader': '$vader'
            }
        }
    ]
    read_database(pipeline, filename)


def vaccine_sentiment_by_day(filename):
    pipeline = [
        {
            '$project': {
                '_id': 0,
                'vader': 1,
                'is_vaccine': 1,
                'uniqueId': 1,
                'year': {
                    '$year': '$date'
                },
                'date': 1
            }
        }, {
            '$match': {
                'is_vaccine': True,
                'year': {
                    '$gte': 2020
                }
            }
        }, {
            '$group': {
                '_id': {
                    'date': '$date',
                    'uniqueId': '$uniqueId'
                },
                'vader': {
                    '$avg': '$vader'
                }
            }
        }, {
            '$group': {
                '_id': '$_id.date',
                'vader': {
                    '$avg': '$vader'
                }
            }
        }, {
            '$sort': {
                '_id': 1
            }
        }, {
            '$project': {
                'period': '$_id',
                'vader': '$vader'
            }
        }
    ]
    read_database(pipeline, filename)


def read_database(pipeline, filename):
    data = get_data(pipeline)
    periods = []
    vader = []
    for i in data:
        periods.append(i['period'])
        vader.append(i['vader'])
    df = pd.DataFrame(data={'Period': periods, 'Vader': vader})
    df.to_csv(filename)


if __name__ == '__main__':
    print("Lets fuck shit up")
    # create_brand_mapping()
    #vaccine_sentiment_by_week('vaccine_sentiment_by_week.csv')
    vaccine_sentiment_by_month('vaccine_sentiment_by_month.csv')
    #vaccine_sentiment_by_day('vaccine_sentiment_by_day.csv')
