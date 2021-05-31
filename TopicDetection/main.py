import time
import pandas as pd

from pymongo import MongoClient

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


def get_all_vaccine_articles():
    pipeline = [
        {
            '$project': {
                '_id': 0,
                'uniqueId': 1,
                'year': {
                    '$year': '$date'
                },
                'normalized_title': 1,
                'normalized_description': 1,
                'is_vaccine': 1
            }
        }, {
            '$match': {
                'year': {
                    '$gte': 2020
                },
                'is_vaccine': True
            }
        }, {
            '$group': {
                '_id': '$uniqueId',
                'title': {
                    '$first': '$normalized_title'
                },
                'description': {
                    '$first': '$normalized_description'
                }
            }
        }, {
            '$project': {
                'text': {
                    '$concatArrays': [
                        '$title', '$description'
                    ]
                }
            }
        }
    ]
    lines_ = []
    for i in get_data(pipeline):
        lines_.append(i['text'])
    return lines_


def get_vaccine_articles(keywords_):
    LABEL = 'is_vaccine'
    or_array = [regex_match('title', keyword, case_sensivity) for (keyword, case_sensivity) in keywords_]
    or_array.extend([regex_match('description', keyword, case_sensivity) for (keyword, case_sensivity) in keywords_])
    pipeline = [
        {
            '$project': {
                '_id': 0,
                'uniqueId': 1,
                'year': {
                    '$year': '$date'
                },
                'normalized_title': 1,
                'normalized_description': 1,
                'title': 1,
                'description': 1,
                LABEL: 1
            }
        }, {
            '$match': {
                'year': {
                    '$gte': 2020
                },
                LABEL: True
            }
        }, {
            '$match': {
                '$or': or_array
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
                        '$title', '$description'
                    ]
                }
            }
        }
    ]
    lines_ = []
    for i in get_data(pipeline):
        lines_.append(i['text'])
    return lines_


def regex_match(field, keyword, case_sensitive):
    return {
        field: {
            '$regex': f'.*{keyword}.*',
            '$options': ('i' if case_sensitive else '')
        }
    }


if __name__ == '__main__':
    #lines = get_all_vaccine_articles()
    #df = pd.DataFrame(data={'text': lines})
    #df.to_csv('all_vaccine.csv')
    brands = {
        #'AstraZeneca': [('AstraZeneca', False)],
        #'Biontech': [('Biontech', False)],
        #'Pfizer': [('Pfizer', False)],
        #'Moderna': [('Moderna', False)],
        #'Sputnik V': [('Sputnik V', False)],
        #'Johnson & Johnson': [('Johnson & Johnson', False), ('Jannsen', False), ('Johnson and Johnson', False)],
        #'EpiVacCorona': [('EpiVacCorona', False)],
        #'CoviVac': [('CoviVac', False)],
        #'GOP': [('GOP', True), ('Republican Party', False)],
        'EU': [('EU', True), ('European Union', False)]
    }
    for brand, keywords in brands.items():
        print(brand)
        print(keywords)
        lines = get_vaccine_articles(keywords)
        df = pd.DataFrame(data={'text': lines})
        df.to_csv(f'./data/{brand}_text.csv')
