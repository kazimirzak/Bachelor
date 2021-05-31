import time

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


if __name__ == '__main__':
    print("Lets fuck shit up!")
    text = "EU regulator says AstraZeneca vaccine safe but can't rule out link to blood clots"
    pipeline = [
        {
            '$match': {
                '$or': [
                    {
                        'title': {
                            '$regex': f'.*{text}.*',
                            '$options': 'i'
                        }
                    }, {
                        'description': {
                            '$regex': f'.*{text}.*',
                            '$options': 'i'
                        }
                    }
                ]
            }
        }
    ]
    for i in get_data(pipeline):
        print(i)
