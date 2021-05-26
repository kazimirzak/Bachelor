from pymongo import MongoClient
import time
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

client = MongoClient('mongodb://localhost:27017')
db = client.Bachelor
sns.set(rc={'figure.figsize': (11, 4)})


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
    print("lets fuck shit up!")
    num_of_covid_day = [
        {
            "$project":
                {
                    "date": 1,
                    "is_covid": 1
                }
        },
        {
            "$match":
                {
                    "is_covid": True
                }
        },
        {
            "$group":
                {
                    "_id": "$date",
                    "num_of_covid": {
                        "$sum": 1
                    }
                }
        },
        {
            "$sort":
                {
                    "_id": 1
                }
        }
    ]
    num_of_vaccine_day = [
        #{"$limit": 1000},
        {
            "$project":
                {
                    "date": 1,
                    "is_vaccine": 1
                }
        },
        {
            "$match":
                {
                    "is_vaccine": True
                }
        },
        {
            "$group":
                {
                    "_id": "$date",
                    "num_of_vaccine": {
                        "$sum": 1
                    }
                }
        },
        {
            "$sort":
                {
                    "_id": 1
                }
        }
    ]
    x = []
    y = []
    for thing in get_data(num_of_vaccine_day):
        x.append(thing['_id'])
        y.append(thing['num_of_vaccine'])
    xlabel = "Date"
    ylabel = "Vaccine articles"
    data = {xlabel: x, ylabel: y}
    df = pd.DataFrame(data=data)
    df = df.set_index('Date')

    #df = df.loc[df.index.year >= 2019]

    plot = df.groupby([df.index.year, df.index.month]).sum().plot(y=ylabel, kind='bar')
    #plot.set_ylabel(ylabel)
    plot.set_xlabel("(Year, Month)")
    plot.set_title("Number of Vaccine articles")
    plt.show()
    print(df.index)
    print(df.head())
    print(df.dtypes)

