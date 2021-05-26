from pymongo import MongoClient
import time
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from numpy import mean

client = MongoClient('mongodb://localhost:27017')
db = client.Bachelor
sns.set(rc={'figure.figsize': (11, 4)})

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


if __name__ == '__main__':
    print("Lets fuck shit up")
    pipeline = [
        {
            '$project': {
                '_id': 0,
                'uniqueId': 1,
                'year': {
                    '$year': '$date'
                },
                'vader': 1,
                'country': 1,
                label: 1

            }
        }, {
            '$match': {
                label: True,
                'year': {
                    '$gt': 2020
                }
            }
        }, {
            '$group': {
                '_id': {
                    'country': '$country',
                    'uniqueId': '$uniqueId'
                },
                'year': {
                    '$max': '$year'
                },
                'vader': {
                    '$first': '$vader'
                }
            }
        }, {
            '$project': {
                '_id': 0,
                'country': '$_id.country',
                'vader': 1
            }
        }, {
            '$sort': {
                'country': 1
            }
        }
    ]
    x = []
    y = []
    for i in get_data(pipeline):
        x.append(i['country'])
        y.append(i['vader'])
    xlabel = "Country"
    ylabel = "Sentiment"
    average = mean(y)
    data = {xlabel: x, ylabel: y}
    df = pd.DataFrame(data=data)
    fig, axes = plt.subplots()
    ax = sns.pointplot(xlabel, ylabel, data=df, ax=axes, estimator=mean, ci=None, color='red', linestyles='', scale=0.8)
    ax.axhline(average, ls='--', color='red')
    plt.setp(ax.lines, zorder=100)
    plt.setp(ax.collections, zorder=100, label="")
    sns.violinplot(xlabel, ylabel, data=df, ax=axes, linewidth=2)
    sns.lineplot()
    axes.set_title("Sentiment of vaccine related articles in 2021")
    axes.set_xlabel(xlabel)
    axes.set_ylabel(ylabel)
    plt.show()
